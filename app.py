# app.py — PostgreSQL persistant
# + ADMIN login (nom + mdp) + page publique HTML
# + Page "Résultats par course" (dropdown GP + détail points)
# + Switch admin ON/OFF pour ouvrir/fermer les pronos (fermé = public)
# + Auto-redirect : si GP fermé, /w/<id>/pronos => /w/<id>/public/pronos
# + MAJ anti-doublons : 1 seule ligne par pseudo (dernier prono)
# + Résultats officiels persistants en DB
# + Classement général saison
# + Gestion admin du prono championnat du monde
# + NOUVEAU : gestion des participants (pseudo autorisé uniquement)
# + NOUVEAU : persistance des pronos basée sur le pseudo autorisé
# + NOUVEAU : authentification joueur par pseudo + code secret
# + NOUVEAU : gestion admin approfondie des pronos GP
# + NOUVEAU : questions bonus dynamiques par GP (persistantes DB)
# + NOUVEAU : gestion admin du calendrier MotoGP (persistante DB)
# + NOUVEAU : suppression admin définitive d’un joueur + ses données liées

import os
import json
import uuid
import re
import unicodedata
from datetime import datetime, date, timedelta
from functools import wraps
from threading import Lock

from flask import (
    Flask, render_template, request, redirect, url_for,
    make_response, flash, session
)
from sqlalchemy import create_engine, text
from werkzeug.security import generate_password_hash, check_password_hash


app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")
app.permanent_session_lifetime = timedelta(days=365)

ADMIN_USER = os.environ.get("ADMIN_USER", "")
ADMIN_PASS = os.environ.get("ADMIN_PASS", "")

BONUS_POINTS_PER_QUESTION = 0.5

DATA_DIR = "data"
PRONOS_DIR = os.path.join(DATA_DIR, "pronos")
RESULTS_DIR = os.path.join(DATA_DIR, "results")
PARTICIPANTS_FILE = os.path.join(DATA_DIR, "participants.json")
WEEKENDS_FILE = os.path.join(DATA_DIR, "weekends.json")

os.makedirs(PRONOS_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)

RIDERS = [
    "#5 Johann Zarco",
    "#7 Toprak Razgatlioglu",
    "#10 Luca Marini",
    "#11 Diogo Moreira",
    "#12 Maverick Vinales",
    "#20 Fabio Quartararo",
    "#21 Franco Morbidelli",
    "#23 Enea Bastianini",
    "#25 Raul Fernandez",
    "#33 Brad Binder",
    "#36 Joan Mir",
    "#37 Pedro Acosta",
    "#42 Alex Rins",
    "#43 Jack Miller",
    "#49 Fabio Di Giannantonio",
    "#54 Fermin Aldeguer",
    "#63 Francesco Bagnaia",
    "#72 Marco Bezzecchi",
    "#73 Alex Marquez",
    "#79 Ai Ogura",
    "#89 Jorge Martin",
    "#93 Marc Marquez",
]

ALL_TEAMS = [
    "Aprilia Racing",
    "Ducati Lenovo Team",
    "Honda HRC Castrol",
    "Monster Energy Yamaha MotoGP",
    "Red Bull KTM Factory Racing",
    "Gresini Racing MotoGP",
    "LCR Honda Castrol",
    "LCR Honda Idemitsu",
    "Pertamina Enduro VR46 Racing Team",
    "Prima Pramac Yamaha MotoGP",
    "Red Bull KTM Tech3",
    "Trackhouse MotoGP Team",
]

OFFICIAL_TEAMS = [
    "Aprilia Racing",
    "Ducati Lenovo Team",
    "Honda HRC Castrol",
    "Monster Energy Yamaha MotoGP",
    "Red Bull KTM Factory Racing",
]

SATELLITE_TEAMS = [
    "Gresini Racing MotoGP",
    "LCR Honda Castrol",
    "LCR Honda Idemitsu",
    "Pertamina Enduro VR46 Racing Team",
    "Prima Pramac Yamaha MotoGP",
    "Red Bull KTM Tech3",
    "Trackhouse MotoGP Team",
]

BONUS_TYPE_CHOICES = [
    {"value": "yes_no", "label": "Oui / Non"},
    {"value": "rider", "label": "Choisir un pilote"},
    {"value": "team", "label": "Choisir une team"},
    {"value": "official_team", "label": "Choisir team officielle"},
    {"value": "satellite_team", "label": "Choisir team satellite"},
    {"value": "range", "label": "Plage numérique"},
    {"value": "number", "label": "Numérique"},
    {"value": "text", "label": "Texte libre"},
]


# ------------------ Utils ------------------
def admin_enabled():
    return bool(ADMIN_USER and ADMIN_PASS)


def is_admin():
    return session.get("is_admin") is True


def require_admin(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not admin_enabled():
            return "Admin non configuré (ADMIN_USER/ADMIN_PASS manquants).", 500
        if not is_admin():
            return redirect(url_for("admin_login"))
        return fn(*args, **kwargs)
    return wrapper


def normalize_pseudo(pseudo: str) -> str:
    return " ".join((pseudo or "").strip().lower().split())


def _parse_dt_maybe(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


def normalize(x):
    return (x or "").strip().lower()


def slugify(value: str) -> str:
    value = (value or "").strip().lower()
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^a-z0-9]+", "-", value).strip("-")
    return value or "item"


def normalize_text_free(value: str) -> str:
    value = (value or "").strip().lower()
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"\s+", " ", value).strip()
    return value


def parse_range_options(raw: str):
    items = []
    for part in (raw or "").split(";"):
        v = part.strip()
        if v:
            items.append(v)
    return items


def get_default_range_options():
    return ["0", "1-2", "3-4", "5-6", "7-8", "9-10", "11+"]


def get_bonus_type_label(type_value: str):
    for item in BONUS_TYPE_CHOICES:
        if item["value"] == type_value:
            return item["label"]
    return type_value or "?"


def get_bonus_options_for_type(qtype: str, question: dict = None):
    qtype = (qtype or "").strip()
    question = question or {}

    if qtype == "yes_no":
        return ["Oui", "Non"]
    if qtype == "rider":
        return list(RIDERS)
    if qtype == "team":
        return list(ALL_TEAMS)
    if qtype == "official_team":
        return list(OFFICIAL_TEAMS)
    if qtype == "satellite_team":
        return list(SATELLITE_TEAMS)
    if qtype == "range":
        opts = question.get("options") or []
        return opts if opts else get_default_range_options()
    return []


def validate_bonus_answer_for_question(question: dict, answer):
    question = question or {}
    qtype = (question.get("type") or "").strip()
    answer = "" if answer is None else str(answer).strip()

    if not qtype:
        return False, "Type de question bonus invalide."

    if qtype in {"yes_no", "rider", "team", "official_team", "satellite_team", "range"}:
        allowed = get_bonus_options_for_type(qtype, question)
        if answer and answer not in allowed:
            return False, f"Réponse non autorisée pour la question '{question.get('label', question.get('id', '?'))}'."
        return True, ""

    if qtype == "number":
        if not answer:
            return True, ""
        try:
            int(answer)
            return True, ""
        except Exception:
            return False, f"Réponse numérique invalide pour '{question.get('label', question.get('id', '?'))}'."

    if qtype == "text":
        return True, ""

    return False, "Type de question bonus non reconnu."


def normalize_bonus_answer_by_type(qtype: str, value):
    qtype = (qtype or "").strip()
    if value is None:
        return ""

    if qtype == "number":
        try:
            return str(int(str(value).strip()))
        except Exception:
            return ""

    if qtype == "text":
        return normalize_text_free(str(value))

    return str(value).strip()


def is_bonus_answer_correct(question: dict, predicted, actual):
    question = question or {}
    qtype = question.get("type") or ""

    if not predicted or not actual:
        return False

    return normalize_bonus_answer_by_type(qtype, predicted) == normalize_bonus_answer_by_type(qtype, actual)


def load_json(path, default):
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except Exception:
            return default


def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ------------------ DB (PostgreSQL) ------------------
DATABASE_URL = os.environ.get("DATABASE_URL")
engine = None
_DB_BOOTSTRAPPED = False
_DB_BOOTSTRAP_LOCK = Lock()

if DATABASE_URL:
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_recycle=300
    )


# ------------------ Weekends sanitize/helpers ------------------
def sanitize_bonus_question(raw_question: dict, fallback_id: str = None):
    raw_question = raw_question or {}
    qid = (raw_question.get("id") or fallback_id or "").strip()
    label = (raw_question.get("label") or "").strip()
    qtype = (raw_question.get("type") or "yes_no").strip()
    correct_answer = raw_question.get("correct_answer", "")
    order = raw_question.get("order")
    options = raw_question.get("options") or []

    if qtype not in {x["value"] for x in BONUS_TYPE_CHOICES}:
        qtype = "yes_no"

    if not qid:
        qid = f"b-{uuid.uuid4().hex[:8]}"

    if qtype == "range":
        cleaned_options = []
        for o in options:
            oo = str(o).strip()
            if oo and oo not in cleaned_options:
                cleaned_options.append(oo)
        options = cleaned_options if cleaned_options else get_default_range_options()
    else:
        options = []

    return {
        "id": qid,
        "label": label,
        "type": qtype,
        "options": options,
        "correct_answer": "" if correct_answer is None else str(correct_answer).strip(),
        "order": int(order) if str(order).isdigit() else 999,
    }


def sanitize_bonus_questions_list(items):
    out = []
    seen = set()
    for idx, raw in enumerate(items or []):
        if not isinstance(raw, dict):
            continue
        q = sanitize_bonus_question(raw, fallback_id=f"b{idx+1}")
        if not q.get("label"):
            continue
        if q["id"] in seen:
            q["id"] = f"{q['id']}-{idx+1}"
        seen.add(q["id"])
        out.append(q)

    out.sort(key=lambda x: (x.get("order", 999), x.get("label", "").lower()))
    return out


def sanitize_weekends_list(weekends):
    out = []
    for w in weekends or []:
        if not isinstance(w, dict):
            continue
        ww = dict(w)
        ww.setdefault("bonus_questions", [])
        ww["bonus_questions"] = sanitize_bonus_questions_list(ww.get("bonus_questions") or [])
        ww["cancelled"] = bool(ww.get("cancelled", False))
        out.append(ww)
    return out


def get_season_year():
    if engine:
        data = load_json(WEEKENDS_FILE, {"season_year": date.today().year})
        y = data.get("season_year")
        if isinstance(y, int):
            return y
        return date.today().year

    data = load_json(WEEKENDS_FILE, {"season_year": date.today().year})
    y = data.get("season_year")
    if isinstance(y, int):
        return y
    return date.today().year


def parse_weekend_date(raw_date: str, season_year: int):
    raw_date = (raw_date or "").strip()
    if not raw_date:
        return None
    try:
        return datetime.strptime(raw_date, "%Y-%m-%d").date()
    except Exception:
        pass
    try:
        mm, dd = raw_date.split("-")
        return date(season_year, int(mm), int(dd))
    except Exception:
        return None


def weekend_status(weekend_date: date, open_days_before: int = 10, cancelled: bool = False):
    if cancelled:
        return "cancelled"
    if not weekend_date:
        return "closed"
    today = date.today()
    if weekend_date < today:
        return "past"
    open_from = weekend_date - timedelta(days=open_days_before)
    if today >= open_from:
        return "open"
    return "closed"


def weekend_sort_key(w: dict):
    season_year = get_season_year()
    d = parse_weekend_date(w.get("date", ""), season_year)
    return (d or date.max, (w.get("label") or "").lower())


def bootstrap_weekends():
    if os.path.exists(WEEKENDS_FILE):
        return

    demo = {
        "season_year": date.today().year,
        "timezone": "Europe/Paris",
        "weekends": [
            {
                "id": "qatar",
                "label": "GP du Qatar",
                "date": f"{date.today().year}-04-12",
                "time": None,
                "cancelled": False,
                "bonus_questions": [
                    {
                        "id": "b1",
                        "label": "Un pilote Ducati sur le podium du GP ?",
                        "type": "yes_no",
                        "options": [],
                        "correct_answer": "",
                        "order": 1
                    },
                    {
                        "id": "b2",
                        "label": "Combien de chutes pendant le GP ?",
                        "type": "range",
                        "options": ["0", "1-2", "3-4", "5-6", "7+"],
                        "correct_answer": "",
                        "order": 2
                    },
                ],
            }
        ],
    }
    os.makedirs(DATA_DIR, exist_ok=True)
    save_json(WEEKENDS_FILE, demo)


bootstrap_weekends()


def db_init():
    if not engine:
        return

    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS weekends (
            weekend_id TEXT PRIMARY KEY,
            closed_at TIMESTAMPTZ NULL,
            pronos_public_at TIMESTAMPTZ NULL,
            results_published_at TIMESTAMPTZ NULL,
            config_json JSONB NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """))

        conn.execute(text("""
            ALTER TABLE weekends
            ADD COLUMN IF NOT EXISTS pronos_public_at TIMESTAMPTZ NULL;
        """))
        conn.execute(text("""
            ALTER TABLE weekends
            ADD COLUMN IF NOT EXISTS results_published_at TIMESTAMPTZ NULL;
        """))
        conn.execute(text("""
            ALTER TABLE weekends
            ADD COLUMN IF NOT EXISTS config_json JSONB NULL;
        """))
        conn.execute(text("""
            ALTER TABLE weekends
            ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS participants (
            id SERIAL PRIMARY KEY,
            pseudo TEXT NOT NULL,
            pseudo_normalized TEXT NOT NULL,
            secret_hash TEXT NULL,
            active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """))

        conn.execute(text("""
            ALTER TABLE participants
            ADD COLUMN IF NOT EXISTS secret_hash TEXT NULL;
        """))

        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_participants_pseudo_normalized
            ON participants(pseudo_normalized);
        """))

        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_participants_active
            ON participants(active);
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS pronos (
            id SERIAL PRIMARY KEY,
            weekend_id TEXT NOT NULL,
            user_key TEXT NOT NULL,
            player_name TEXT NOT NULL,
            payload_json JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """))

        conn.execute(text("""
            ALTER TABLE pronos
            ADD COLUMN IF NOT EXISTS player_norm TEXT NULL;
        """))
        conn.execute(text("""
            ALTER TABLE pronos
            ADD COLUMN IF NOT EXISTS participant_id INTEGER NULL;
        """))

        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_pronos_weekend
            ON pronos(weekend_id);
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_pronos_weekend_player
            ON pronos(weekend_id, player_name);
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_pronos_weekend_player_norm
            ON pronos(weekend_id, player_norm);
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_pronos_participant_id
            ON pronos(participant_id);
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_pronos_updated_at
            ON pronos(updated_at);
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS championnat_pronos (
            id SERIAL PRIMARY KEY,
            user_key TEXT NOT NULL,
            player_name TEXT NOT NULL,
            payload_json JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """))

        conn.execute(text("""
            ALTER TABLE championnat_pronos
            ADD COLUMN IF NOT EXISTS player_norm TEXT NULL;
        """))
        conn.execute(text("""
            ALTER TABLE championnat_pronos
            ADD COLUMN IF NOT EXISTS participant_id INTEGER NULL;
        """))

        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_championnat_pronos_player_norm
            ON championnat_pronos(player_norm);
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_championnat_pronos_participant_id
            ON championnat_pronos(participant_id);
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS championnat_state (
            id INTEGER PRIMARY KEY,
            is_open BOOLEAN NOT NULL DEFAULT TRUE,
            revealed BOOLEAN NOT NULL DEFAULT FALSE,
            locked_at TIMESTAMPTZ NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """))

        conn.execute(text("""
            INSERT INTO championnat_state (id, is_open, revealed, locked_at, updated_at)
            VALUES (1, TRUE, FALSE, NULL, NOW())
            ON CONFLICT (id) DO NOTHING
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS results (
            weekend_id TEXT PRIMARY KEY,
            payload_json JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """))

        conn.execute(text("""
            UPDATE pronos
            SET player_norm = LOWER(BTRIM(player_name))
            WHERE player_norm IS NULL
        """))

        conn.execute(text("""
            UPDATE championnat_pronos
            SET player_norm = LOWER(BTRIM(player_name))
            WHERE player_norm IS NULL
        """))


def seed_weekends_config_to_db_if_needed():
    if not engine:
        return

    data = load_json(WEEKENDS_FILE, {"weekends": []})
    if isinstance(data, list):
        data = {"weekends": data}
    if not isinstance(data, dict):
        data = {"weekends": []}

    weekends = sanitize_weekends_list(data.get("weekends", []))

    with engine.begin() as conn:
        for w in weekends:
            wid = (w.get("id") or "").strip()
            if not wid:
                continue

            row = conn.execute(text("""
                SELECT config_json
                FROM weekends
                WHERE weekend_id=:w
                LIMIT 1
            """), {"w": wid}).fetchone()

            if row and row[0]:
                continue

            conn.execute(text("""
                INSERT INTO weekends (weekend_id, config_json, updated_at)
                VALUES (:w, CAST(:cfg AS jsonb), NOW())
                ON CONFLICT (weekend_id)
                DO UPDATE SET
                    config_json = COALESCE(weekends.config_json, EXCLUDED.config_json),
                    updated_at = NOW()
            """), {
                "w": wid,
                "cfg": json.dumps(w, ensure_ascii=False)
            })


def ensure_db_bootstrap():
    global _DB_BOOTSTRAPPED

    if _DB_BOOTSTRAPPED:
        return

    with _DB_BOOTSTRAP_LOCK:
        if _DB_BOOTSTRAPPED:
            return

        if not engine:
            _DB_BOOTSTRAPPED = True
            return

        db_init()
        seed_weekends_config_to_db_if_needed()
        _DB_BOOTSTRAPPED = True


def load_weekends_data():
    file_data = load_json(WEEKENDS_FILE, {"weekends": []})
    if isinstance(file_data, list):
        file_data = {"weekends": file_data}
    if not isinstance(file_data, dict):
        file_data = {"weekends": []}

    base_weekends = sanitize_weekends_list(file_data.get("weekends", []))
    by_id = {}
    for w in base_weekends:
        wid = (w.get("id") or "").strip()
        if wid:
            by_id[wid] = dict(w)

    if engine:
        ensure_db_bootstrap()

        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT weekend_id, config_json
                FROM weekends
                ORDER BY weekend_id
            """)).fetchall()

        for r in rows:
            wid = r[0]
            cfg = dict(r[1] or {}) if r[1] else {}
            if wid not in by_id:
                by_id[wid] = {"id": wid}
            merged = dict(by_id[wid])
            merged.update(cfg or {})
            merged["id"] = wid
            by_id[wid] = merged

    return {
        "season_year": file_data.get("season_year", date.today().year),
        "timezone": file_data.get("timezone", "Europe/Paris"),
        "weekends": sanitize_weekends_list(list(by_id.values()))
    }


def save_weekends_data(data: dict):
    data = dict(data or {})
    weekends = sanitize_weekends_list(data.get("weekends", []))

    file_payload = load_json(WEEKENDS_FILE, {"season_year": date.today().year, "timezone": "Europe/Paris", "weekends": []})
    if isinstance(file_payload, list):
        file_payload = {"weekends": file_payload}
    if not isinstance(file_payload, dict):
        file_payload = {"weekends": []}
    file_payload["season_year"] = data.get("season_year", file_payload.get("season_year", date.today().year))
    file_payload["timezone"] = data.get("timezone", file_payload.get("timezone", "Europe/Paris"))
    file_payload["weekends"] = weekends
    save_json(WEEKENDS_FILE, file_payload)

    if engine:
        ensure_db_bootstrap()
        with engine.begin() as conn:
            for w in weekends:
                wid = (w.get("id") or "").strip()
                if not wid:
                    continue
                ww = dict(w)
                ww["id"] = wid
                conn.execute(text("""
                    INSERT INTO weekends (weekend_id, config_json, updated_at)
                    VALUES (:w, CAST(:cfg AS jsonb), NOW())
                    ON CONFLICT (weekend_id)
                    DO UPDATE SET
                        config_json = EXCLUDED.config_json,
                        updated_at = NOW()
                """), {
                    "w": wid,
                    "cfg": json.dumps(ww, ensure_ascii=False)
                })


def load_weekends_list():
    return load_weekends_data().get("weekends", [])


def get_weekend(weekend_id):
    for w in load_weekends_list():
        if w.get("id") == weekend_id:
            w = dict(w)
            w.setdefault("bonus_questions", [])
            w["bonus_questions"] = sanitize_bonus_questions_list(w.get("bonus_questions", []))
            w["cancelled"] = bool(w.get("cancelled", False))
            for q in w["bonus_questions"]:
                q["type_label"] = get_bonus_type_label(q.get("type"))
                q["resolved_options"] = get_bonus_options_for_type(q.get("type"), q)
            return w
    return None


def get_sorted_weekends():
    items = []
    for w in load_weekends_list():
        ww = dict(w)
        ww["cancelled"] = bool(ww.get("cancelled", False))
        ww["date_obj"] = parse_weekend_date(ww.get("date", ""), get_season_year())
        items.append(ww)
    items.sort(key=weekend_sort_key)
    return items


def update_weekend_bonus_questions(weekend_id: str, new_questions: list):
    data = load_weekends_data()
    weekends = data.get("weekends", [])
    found = False
    for w in weekends:
        if w.get("id") == weekend_id:
            w["bonus_questions"] = sanitize_bonus_questions_list(new_questions)
            found = True
            break
    if not found:
        return False
    save_weekends_data(data)
    return True


def add_bonus_question_to_weekend(weekend_id: str, label: str, qtype: str, options_raw: str = ""):
    data = load_weekends_data()
    weekends = data.get("weekends", [])
    for w in weekends:
        if w.get("id") == weekend_id:
            questions = sanitize_bonus_questions_list(w.get("bonus_questions", []))
            next_order = max([q.get("order", 0) for q in questions] + [0]) + 1

            qtype = (qtype or "").strip()
            if qtype not in {x["value"] for x in BONUS_TYPE_CHOICES}:
                qtype = "yes_no"

            options = parse_range_options(options_raw) if qtype == "range" else []
            if qtype == "range" and not options:
                options = get_default_range_options()

            qid = f"b-{uuid.uuid4().hex[:8]}"
            questions.append({
                "id": qid,
                "label": (label or "").strip(),
                "type": qtype,
                "options": options,
                "correct_answer": "",
                "order": next_order,
            })
            w["bonus_questions"] = questions
            save_weekends_data(data)
            return True
    return False


def update_bonus_question_in_weekend(weekend_id: str, question_id: str, label: str, qtype: str, options_raw: str = ""):
    data = load_weekends_data()
    weekends = data.get("weekends", [])
    for w in weekends:
        if w.get("id") == weekend_id:
            questions = sanitize_bonus_questions_list(w.get("bonus_questions", []))
            found = False
            for q in questions:
                if q.get("id") == question_id:
                    found = True
                    q["label"] = (label or "").strip()
                    q["type"] = (qtype or "").strip()
                    if q["type"] not in {x["value"] for x in BONUS_TYPE_CHOICES}:
                        q["type"] = "yes_no"
                    q["options"] = parse_range_options(options_raw) if q["type"] == "range" else []
                    if q["type"] == "range" and not q["options"]:
                        q["options"] = get_default_range_options()

                    correct = q.get("correct_answer")
                    ok, _ = validate_bonus_answer_for_question(q, correct)
                    if not ok:
                        q["correct_answer"] = ""
                    break

            if not found:
                return False

            w["bonus_questions"] = questions
            save_weekends_data(data)
            return True
    return False


def delete_bonus_question_from_weekend(weekend_id: str, question_id: str):
    data = load_weekends_data()
    weekends = data.get("weekends", [])
    changed = False
    for w in weekends:
        if w.get("id") == weekend_id:
            before = len(w.get("bonus_questions", []))
            w["bonus_questions"] = [
                q for q in sanitize_bonus_questions_list(w.get("bonus_questions", []))
                if q.get("id") != question_id
            ]
            after = len(w["bonus_questions"])
            if after != before:
                changed = True
            break

    if changed:
        save_weekends_data(data)
    return changed


def is_weekend_cancelled(weekend_or_id) -> bool:
    if isinstance(weekend_or_id, dict):
        return bool(weekend_or_id.get("cancelled", False))
    w = get_weekend(str(weekend_or_id))
    return bool(w and w.get("cancelled", False))


def update_weekend_calendar(weekend_id: str, new_date: str = None, new_label: str = None):
    data = load_weekends_data()
    weekends = data.get("weekends", [])

    for w in weekends:
        if w.get("id") == weekend_id:
            if new_label is not None:
                w["label"] = (new_label or "").strip() or w.get("label", weekend_id)

            if new_date is not None:
                new_date = (new_date or "").strip()
                if not new_date:
                    return False, "La date est obligatoire."

                parsed = parse_weekend_date(new_date, get_season_year())
                if not parsed:
                    return False, "Date invalide. Format attendu : YYYY-MM-DD."

                w["date"] = parsed.isoformat()

            save_weekends_data(data)
            return True, "GP mis à jour."

    return False, "Week-end introuvable."


def set_weekend_cancelled_flag(weekend_id: str, cancelled: bool):
    data = load_weekends_data()
    weekends = data.get("weekends", [])

    for w in weekends:
        if w.get("id") == weekend_id:
            w["cancelled"] = bool(cancelled)
            save_weekends_data(data)
            return True, "Statut du GP mis à jour."

    return False, "Week-end introuvable."


# ------------------ Participants helpers ------------------
def load_participants_fallback():
    data = load_json(PARTICIPANTS_FILE, {"participants": []})
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("participants", [])
    return []


def save_participants_fallback(items):
    save_json(PARTICIPANTS_FILE, {"participants": items or []})


def get_all_participants(include_inactive=True):
    if engine:
        ensure_db_bootstrap()
        with engine.begin() as conn:
            if include_inactive:
                rows = conn.execute(text("""
                    SELECT id, pseudo, pseudo_normalized, secret_hash, active, created_at
                    FROM participants
                    ORDER BY pseudo ASC
                """)).fetchall()
            else:
                rows = conn.execute(text("""
                    SELECT id, pseudo, pseudo_normalized, secret_hash, active, created_at
                    FROM participants
                    WHERE active = TRUE
                    ORDER BY pseudo ASC
                """)).fetchall()

        return [{
            "id": r[0],
            "pseudo": r[1],
            "pseudo_normalized": r[2],
            "secret_hash": r[3],
            "active": bool(r[4]),
            "created_at": str(r[5]) if r[5] else None
        } for r in rows]

    items = load_participants_fallback()
    out = []
    for i, p in enumerate(items, start=1):
        active = bool(p.get("active", True))
        if include_inactive or active:
            out.append({
                "id": p.get("id", i),
                "pseudo": p.get("pseudo", ""),
                "pseudo_normalized": p.get("pseudo_normalized") or normalize_pseudo(p.get("pseudo", "")),
                "secret_hash": p.get("secret_hash"),
                "active": active,
                "created_at": p.get("created_at"),
            })
    out.sort(key=lambda x: (x.get("pseudo") or "").lower())
    return out


def get_active_participants():
    return get_all_participants(include_inactive=False)


def get_participant_by_input(pseudo_input: str, active_only=True):
    pseudo_norm = normalize_pseudo(pseudo_input)
    if not pseudo_norm:
        return None

    if engine:
        ensure_db_bootstrap()
        with engine.begin() as conn:
            if active_only:
                row = conn.execute(text("""
                    SELECT id, pseudo, pseudo_normalized, secret_hash, active, created_at
                    FROM participants
                    WHERE pseudo_normalized=:pn AND active=TRUE
                    LIMIT 1
                """), {"pn": pseudo_norm}).fetchone()
            else:
                row = conn.execute(text("""
                    SELECT id, pseudo, pseudo_normalized, secret_hash, active, created_at
                    FROM participants
                    WHERE pseudo_normalized=:pn
                    LIMIT 1
                """), {"pn": pseudo_norm}).fetchone()

        if not row:
            return None

        return {
            "id": row[0],
            "pseudo": row[1],
            "pseudo_normalized": row[2],
            "secret_hash": row[3],
            "active": bool(row[4]),
            "created_at": str(row[5]) if row[5] else None
        }

    for p in get_all_participants(include_inactive=not active_only):
        if p["pseudo_normalized"] == pseudo_norm:
            return p
    return None


def verify_participant_secret(participant: dict, secret_input: str) -> bool:
    if not participant:
        return False

    stored = (participant.get("secret_hash") or "").strip()
    secret_input = (secret_input or "").strip()

    if not stored or not secret_input:
        return False

    try:
        return check_password_hash(stored, secret_input)
    except Exception:
        return False


def add_participant(pseudo: str, secret: str):
    pseudo = (pseudo or "").strip()
    secret = (secret or "").strip()
    pseudo_norm = normalize_pseudo(pseudo)

    if not pseudo or not pseudo_norm:
        return False, "Le pseudo est obligatoire."

    if not secret:
        return False, "Le code secret est obligatoire."

    existing = get_participant_by_input(pseudo, active_only=False)
    if existing:
        return False, "Ce pseudo existe déjà."

    secret_hash = generate_password_hash(secret)

    if engine:
        ensure_db_bootstrap()
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO participants (pseudo, pseudo_normalized, secret_hash, active, created_at)
                VALUES (:p, :pn, :sh, TRUE, NOW())
            """), {"p": pseudo, "pn": pseudo_norm, "sh": secret_hash})
        return True, "Joueur ajouté."

    items = load_participants_fallback()
    items.append({
        "id": len(items) + 1,
        "pseudo": pseudo,
        "pseudo_normalized": pseudo_norm,
        "secret_hash": secret_hash,
        "active": True,
        "created_at": datetime.utcnow().isoformat()
    })
    save_participants_fallback(items)
    return True, "Joueur ajouté."


def reset_participant_secret(participant_id: int, new_secret: str):
    new_secret = (new_secret or "").strip()
    if not new_secret:
        return False, "Le nouveau code secret est obligatoire."

    secret_hash = generate_password_hash(new_secret)

    if engine:
        ensure_db_bootstrap()
        with engine.begin() as conn:
            row = conn.execute(text("""
                SELECT id, pseudo
                FROM participants
                WHERE id=:pid
                LIMIT 1
            """), {"pid": participant_id}).fetchone()

            if not row:
                return False, "Joueur introuvable."

            conn.execute(text("""
                UPDATE participants
                SET secret_hash=:sh
                WHERE id=:pid
            """), {"sh": secret_hash, "pid": participant_id})

        return True, f"Code secret mis à jour pour {row[1]}."

    items = load_participants_fallback()
    found = None
    for p in items:
        if int(p.get("id", 0)) == int(participant_id):
            p["secret_hash"] = secret_hash
            found = p
            break

    if not found:
        return False, "Joueur introuvable."

    save_participants_fallback(items)
    return True, f"Code secret mis à jour pour {found.get('pseudo', '??')}."


def toggle_participant(participant_id: int):
    if engine:
        ensure_db_bootstrap()
        with engine.begin() as conn:
            row = conn.execute(text("""
                SELECT id, pseudo, active
                FROM participants
                WHERE id=:pid
                LIMIT 1
            """), {"pid": participant_id}).fetchone()

            if not row:
                return False, "Joueur introuvable."

            new_state = not bool(row[2])
            conn.execute(text("""
                UPDATE participants
                SET active=:a
                WHERE id=:pid
            """), {"a": new_state, "pid": participant_id})

        return True, f"Statut mis à jour pour {row[1]}."

    items = load_participants_fallback()
    found = None
    for p in items:
        if int(p.get("id", 0)) == int(participant_id):
            p["active"] = not bool(p.get("active", True))
            found = p
            break

    if not found:
        return False, "Joueur introuvable."

    save_participants_fallback(items)
    return True, f"Statut mis à jour pour {found.get('pseudo', '??')}."


def delete_participant(participant_id: int):
    if engine:
        ensure_db_bootstrap()
        with engine.begin() as conn:
            row = conn.execute(text("""
                SELECT id, pseudo, pseudo_normalized
                FROM participants
                WHERE id=:pid
                LIMIT 1
            """), {"pid": participant_id}).fetchone()

            if not row:
                return False, "Joueur introuvable."

            pseudo = row[1]
            pseudo_norm = row[2] or normalize_pseudo(pseudo)

            conn.execute(text("""
                DELETE FROM pronos
                WHERE participant_id=:pid
                   OR COALESCE(player_norm, LOWER(BTRIM(player_name)))=:pn
            """), {"pid": participant_id, "pn": pseudo_norm})

            conn.execute(text("""
                DELETE FROM championnat_pronos
                WHERE participant_id=:pid
                   OR COALESCE(player_norm, LOWER(BTRIM(player_name)))=:pn
            """), {"pid": participant_id, "pn": pseudo_norm})

            conn.execute(text("""
                DELETE FROM participants
                WHERE id=:pid
            """), {"pid": participant_id})

        return True, f"Joueur supprimé définitivement : {pseudo}"

    items = load_participants_fallback()
    found = None
    kept = []

    for p in items:
        if int(p.get("id", 0)) == int(participant_id):
            found = p
        else:
            kept.append(p)

    if not found:
        return False, "Joueur introuvable."

    pseudo = found.get("pseudo", "")
    pseudo_norm = found.get("pseudo_normalized") or normalize_pseudo(pseudo)

    for weekend in load_weekends_list():
        wid = (weekend.get("id") or "").strip()
        if not wid:
            continue

        all_pronos = load_json(pronos_path(wid), {})
        if isinstance(all_pronos, dict):
            to_delete = []
            for k, v in all_pronos.items():
                if normalize_pseudo(v.get("player_name")) == pseudo_norm:
                    to_delete.append(k)
            for k in to_delete:
                all_pronos.pop(k, None)
            save_json(pronos_path(wid), all_pronos)

    all_preds = load_json(championnat_path(), {})
    if isinstance(all_preds, dict):
        to_delete = []
        for k, v in all_preds.items():
            if normalize_pseudo(v.get("player_name")) == pseudo_norm:
                to_delete.append(k)
        for k in to_delete:
            all_preds.pop(k, None)
        save_json(championnat_path(), all_preds)

    save_participants_fallback(kept)
    return True, f"Joueur supprimé définitivement : {pseudo}"


def get_current_participant(req):
    raw_name = (req.cookies.get("player_name") or "").strip()
    if not raw_name:
        return None
    return get_participant_by_input(raw_name, active_only=True)


# ------------------ Championnat state ------------------
def get_championnat_state():
    default = {
        "is_open": True,
        "revealed": False,
        "locked_at": None,
    }

    if engine:
        ensure_db_bootstrap()
        with engine.begin() as conn:
            row = conn.execute(text("""
                SELECT is_open, revealed, locked_at
                FROM championnat_state
                WHERE id=1
            """)).fetchone()

        if row:
            return {
                "is_open": bool(row[0]),
                "revealed": bool(row[1]),
                "locked_at": str(row[2]) if row[2] else None,
            }
        return default

    path = os.path.join(DATA_DIR, "championnat_state.json")
    return load_json(path, default)


def save_championnat_state(state: dict):
    state = {
        "is_open": bool(state.get("is_open", True)),
        "revealed": bool(state.get("revealed", False)),
        "locked_at": state.get("locked_at"),
    }

    if engine:
        ensure_db_bootstrap()
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO championnat_state (id, is_open, revealed, locked_at, updated_at)
                VALUES (1, :is_open, :revealed, :locked_at, NOW())
                ON CONFLICT (id)
                DO UPDATE SET
                    is_open = EXCLUDED.is_open,
                    revealed = EXCLUDED.revealed,
                    locked_at = EXCLUDED.locked_at,
                    updated_at = NOW()
            """), state)
    else:
        path = os.path.join(DATA_DIR, "championnat_state.json")
        save_json(path, state)


def close_championnat():
    state = get_championnat_state()
    state["is_open"] = False
    state["locked_at"] = datetime.utcnow().isoformat()
    save_championnat_state(state)


def open_championnat():
    state = get_championnat_state()
    state["is_open"] = True
    state["revealed"] = False
    state["locked_at"] = None
    save_championnat_state(state)


def reveal_championnat():
    state = get_championnat_state()
    state["revealed"] = True
    save_championnat_state(state)


def hide_championnat():
    state = get_championnat_state()
    state["revealed"] = False
    save_championnat_state(state)


def get_latest_championnat_pronos_by_player() -> list:
    if engine:
        ensure_db_bootstrap()
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT DISTINCT ON (COALESCE(player_norm, LOWER(BTRIM(player_name))))
                    player_name, payload_json, created_at, updated_at
                FROM championnat_pronos
                ORDER BY COALESCE(player_norm, LOWER(BTRIM(player_name))), updated_at DESC
            """)).fetchall()

        items = []
        for r in rows:
            items.append({
                "player_name": r[0] or "??",
                "payload": dict(r[1] or {}),
                "created_at": str(r[2]) if r[2] else None,
                "updated_at": str(r[3]) if r[3] else None,
            })
        return items

    all_preds = load_json(championnat_path(), {})
    tmp = []
    if isinstance(all_preds, dict):
        for _, p in all_preds.items():
            tmp.append({
                "player_name": p.get("player_name", "??"),
                "payload": dict(p or {}),
                "created_at": p.get("_created_at") or p.get("created_at"),
                "updated_at": p.get("_updated_at") or p.get("updated_at"),
            })
    return dedupe_pronos_by_playername(tmp)


# ------------------ Identification joueurs (cookies) ------------------
def current_player(req):
    participant = get_current_participant(req)
    name = participant["pseudo"] if participant else (req.cookies.get("player_name") or "").strip()
    pid = req.cookies.get("player_id")
    if not pid:
        pid = str(uuid.uuid4())
    return name, pid


# ------------------ Week-end open/close DB state ------------------
def is_weekend_closed(weekend_id: str) -> bool:
    if not engine:
        return False
    ensure_db_bootstrap()
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT closed_at FROM weekends WHERE weekend_id=:w"),
            {"w": weekend_id}
        ).fetchone()
    return bool(row and row[0])


def is_pronos_public(weekend_id: str) -> bool:
    if not engine:
        return False
    ensure_db_bootstrap()
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT pronos_public_at FROM weekends WHERE weekend_id=:w"),
            {"w": weekend_id}
        ).fetchone()
    return bool(row and row[0])


def set_weekend_open(weekend_id: str):
    if not engine:
        return
    ensure_db_bootstrap()
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO weekends (weekend_id, closed_at, pronos_public_at, updated_at)
            VALUES (:w, NULL, NULL, NOW())
            ON CONFLICT (weekend_id)
            DO UPDATE SET
              closed_at = NULL,
              pronos_public_at = NULL,
              updated_at = NOW()
        """), {"w": weekend_id})


def set_weekend_closed_and_public(weekend_id: str):
    if not engine:
        return
    ensure_db_bootstrap()
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO weekends (weekend_id, closed_at, pronos_public_at, updated_at)
            VALUES (:w, NOW(), NOW(), NOW())
            ON CONFLICT (weekend_id)
            DO UPDATE SET
              closed_at = NOW(),
              pronos_public_at = NOW(),
              updated_at = NOW()
        """), {"w": weekend_id})


# ------------------ Points ------------------
def podium_points(pred, actual, well_placed, mis_placed, bonus_exact, bonus_all):
    p = [normalize(x) for x in pred]
    a = [normalize(x) for x in (actual or [])]
    score = 0.0

    for i in range(3):
        if i < len(a) and p[i] and p[i] == a[i]:
            score += well_placed

    for i in range(3):
        if p[i] and p[i] in a and (i >= len(a) or p[i] != a[i]):
            score += mis_placed

    if len(a) >= 3 and all(p[i] == a[i] for i in range(3)):
        score += bonus_exact
    elif len(a) >= 3 and set(p) == set(a):
        score += bonus_all

    return score


def qualif_points(pole_pred, pole_real, q1_preds, q1_actual):
    score = 0.0
    if normalize(pole_pred) == normalize(pole_real):
        score += 2.0
    real_set = {normalize(x) for x in (q1_actual or [])}
    for p in (q1_preds or []):
        if normalize(p) in real_set:
            score += 0.5
    return score


def podium_detail(pred, actual, well_placed, mis_placed, bonus_exact, bonus_all):
    p = [normalize(x) for x in (pred or [])]
    a = [normalize(x) for x in (actual or [])]

    pos = [0.0, 0.0, 0.0]
    for i in range(3):
        if i < len(a) and i < len(p) and p[i] and p[i] == a[i]:
            pos[i] += float(well_placed)

    for i in range(3):
        if i < len(p) and p[i] and p[i] in a and (i >= len(a) or p[i] != a[i]):
            pos[i] += float(mis_placed)

    bonus = 0.0
    bonus_label = None
    if len(a) >= 3 and len(p) >= 3 and all(p[i] == a[i] for i in range(3)):
        bonus = float(bonus_exact)
        bonus_label = "exact"
    elif len(a) >= 3 and len(p) >= 3 and set(p) == set(a):
        bonus = float(bonus_all)
        bonus_label = "all"

    total = round(sum(pos) + bonus, 2)

    return {
        "p1": round(pos[0], 2),
        "p2": round(pos[1], 2),
        "p3": round(pos[2], 2),
        "bonus": round(bonus, 2),
        "bonus_label": bonus_label,
        "total": total
    }


def qualif_detail(pole_pred, pole_real, q1_preds, q1_actual):
    pole_ok = normalize(pole_pred) == normalize(pole_real)
    pole_pts = 2.0 if pole_ok else 0.0

    real_set = {normalize(x) for x in (q1_actual or [])}
    q1 = []
    q1_pts = 0.0
    for x in (q1_preds or []):
        ok = normalize(x) in real_set if x else False
        pts = 0.5 if ok else 0.0
        q1.append({"pick": x, "ok": ok, "pts": pts})
        q1_pts += pts

    total = round(pole_pts + q1_pts, 2)
    return {
        "pole_ok": pole_ok,
        "pole_pts": pole_pts,
        "q1": q1,
        "q1_pts": round(q1_pts, 2),
        "total": total
    }


def bonus_detail(pred_bonus: dict, real_bonus: dict, weekend_bonus_questions: list):
    pred_bonus = pred_bonus or {}
    real_bonus = real_bonus or {}
    items = []
    total = 0.0

    for b in (weekend_bonus_questions or []):
        bid = b.get("id")
        if not bid:
            continue

        pred = pred_bonus.get(bid)
        real = real_bonus.get(bid)
        ok = is_bonus_answer_correct(b, pred, real)
        pts = BONUS_POINTS_PER_QUESTION if ok else 0.0
        total += pts

        items.append({
            "id": bid,
            "label": b.get("label", bid),
            "type": b.get("type"),
            "type_label": get_bonus_type_label(b.get("type")),
            "pred": pred,
            "real": real,
            "ok": ok,
            "pts": pts
        })

    return {"items": items, "total": round(total, 2)}


def compute_points_breakdown(prono: dict, results: dict, w: dict):
    prono = prono or {}
    results = results or {}
    w = w or {}
    bq = w.get("bonus_questions", []) or []

    qual = qualif_detail(
        prono.get("pole"),
        results.get("pole"),
        [prono.get("q1_1"), prono.get("q1_2")],
        results.get("q1", [])
    )

    sprint = podium_detail(
        [prono.get("sprint_p1"), prono.get("sprint_p2"), prono.get("sprint_p3")],
        results.get("sprint", []),
        well_placed=1.0, mis_placed=0.5, bonus_exact=3.0, bonus_all=1.5
    )

    gp = podium_detail(
        [prono.get("gp_p1"), prono.get("gp_p2"), prono.get("gp_p3")],
        results.get("gp", []),
        well_placed=2.0, mis_placed=1.0, bonus_exact=6.0, bonus_all=3.0
    )

    bonus = bonus_detail(prono.get("bonus", {}), results.get("bonus", {}), bq)

    total = round(qual["total"] + sprint["total"] + gp["total"] + bonus["total"], 2)

    return {
        "qualif": qual,
        "sprint": sprint,
        "gp": gp,
        "bonus": bonus,
        "total": total
    }


# ------------------ Fichiers fallback ------------------
def pronos_path(weekend_id):
    return os.path.join(PRONOS_DIR, f"{weekend_id}.json")


def results_path(weekend_id):
    return os.path.join(RESULTS_DIR, f"{weekend_id}.json")


def championnat_path():
    return os.path.join(PRONOS_DIR, "championnat.json")


def load_results(weekend_id: str):
    if engine:
        ensure_db_bootstrap()
        with engine.begin() as conn:
            row = conn.execute(text("""
                SELECT payload_json
                FROM results
                WHERE weekend_id=:w
            """), {"w": weekend_id}).fetchone()
        if row and row[0]:
            data = dict(row[0] or {})
        else:
            data = None
    else:
        data = load_json(results_path(weekend_id), None)

    if not data:
        return data

    data.setdefault("bonus", {})
    w = get_weekend(weekend_id)
    if w:
        allowed_ids = {q["id"] for q in w.get("bonus_questions", [])}
        data["bonus"] = {k: v for k, v in (data.get("bonus") or {}).items() if k in allowed_ids}
    return data


def save_results(weekend_id: str, results: dict):
    results = results or {}
    if engine:
        ensure_db_bootstrap()
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO results (weekend_id, payload_json, updated_at)
                VALUES (:w, CAST(:p AS jsonb), NOW())
                ON CONFLICT (weekend_id)
                DO UPDATE SET
                    payload_json = EXCLUDED.payload_json,
                    updated_at = NOW()
            """), {"w": weekend_id, "p": json.dumps(results, ensure_ascii=False)})
    save_json(results_path(weekend_id), results)


# ------------------ Helpers anti-doublons ------------------
def dedupe_pronos_by_playername(items):
    best = {}
    for it in items or []:
        name = (it.get("player_name") or it.get("player") or "").strip()
        if not name:
            name = "??"
        dt = (
            _parse_dt_maybe(it.get("updated_at"))
            or _parse_dt_maybe(it.get("_updated_at"))
            or _parse_dt_maybe(it.get("created_at"))
            or datetime.min
        )
        cur = best.get(name)
        if (cur is None) or (dt > cur["_dt"]):
            it2 = dict(it)
            it2["_dt"] = dt
            best[name] = it2

    out = list(best.values())
    out.sort(key=lambda x: x.get("_dt") or datetime.min, reverse=True)
    for x in out:
        x.pop("_dt", None)
    return out


def get_latest_pronos_by_player_for_weekend(weekend_id: str) -> dict:
    out = {}

    if engine:
        ensure_db_bootstrap()
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT DISTINCT ON (COALESCE(player_norm, LOWER(BTRIM(player_name))))
                    player_name, payload_json, updated_at
                FROM pronos
                WHERE weekend_id=:w
                ORDER BY COALESCE(player_norm, LOWER(BTRIM(player_name))), updated_at DESC
            """), {"w": weekend_id}).fetchall()

        for r in rows:
            out[(r[0] or "??")] = dict(r[1] or {})
        return out

    all_pronos = load_json(pronos_path(weekend_id), {})
    tmp = []
    if isinstance(all_pronos, dict):
        for _, p in all_pronos.items():
            tmp.append({
                "player_name": p.get("player_name", "??"),
                "payload": dict(p or {}),
                "updated_at": p.get("_updated_at") or p.get("updated_at") or p.get("created_at"),
            })
    tmp = dedupe_pronos_by_playername(tmp)
    for it in tmp:
        out[it["player_name"]] = it["payload"]
    return out


def count_distinct_pronos_for_weekend(weekend_id: str) -> int:
    if engine:
        ensure_db_bootstrap()
        with engine.begin() as conn:
            row = conn.execute(text("""
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT ON (COALESCE(player_norm, LOWER(BTRIM(player_name))))
                        COALESCE(player_norm, LOWER(BTRIM(player_name)))
                    FROM pronos
                    WHERE weekend_id=:w
                    ORDER BY COALESCE(player_norm, LOWER(BTRIM(player_name))), updated_at DESC
                ) t
            """), {"w": weekend_id}).fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    all_pronos = load_json(pronos_path(weekend_id), {})
    if isinstance(all_pronos, dict):
        tmp = []
        for _, p in all_pronos.items():
            tmp.append({
                "player_name": (p.get("player_name") or "??"),
                "payload": dict(p or {}),
                "updated_at": p.get("_updated_at") or p.get("updated_at") or p.get("created_at")
            })
        return len(dedupe_pronos_by_playername(tmp))
    return 0


@app.before_request
def _bootstrap_once_before_request():
    ensure_db_bootstrap()


# ================== PUBLIC ROUTES ==================
@app.route("/")
def home():
    name, _ = current_player(request)

    weekends = []
    for w in get_sorted_weekends():
        w2 = dict(w)
        w2["status"] = weekend_status(
            w2.get("date_obj"),
            open_days_before=10,
            cancelled=bool(w2.get("cancelled", False))
        )
        w2["closed_db"] = is_weekend_closed(w2.get("id", "")) if engine else False
        weekends.append(w2)

    return render_template(
        "index.html",
        name=name,
        weekends=weekends,
        admin_enabled=admin_enabled(),
        is_admin=is_admin()
    )


@app.route("/login", methods=["GET", "POST"])
def login():
    active_participants = get_active_participants()

    if request.method == "POST":
        pseudo_input = (request.form.get("name") or request.form.get("pseudo") or "").strip()
        secret_input = (request.form.get("secret") or "").strip()

        participant = get_participant_by_input(pseudo_input, active_only=True)

        if not participant:
            flash("Pseudo non reconnu. Merci de choisir un pseudo autorisé.")
            return redirect(url_for("login"))

        if not verify_participant_secret(participant, secret_input):
            flash("Code secret incorrect.")
            return redirect(url_for("login"))

        _, pid = current_player(request)
        resp = make_response(redirect(url_for("home")))
        resp.set_cookie("player_name", participant["pseudo"], max_age=60 * 60 * 24 * 365)
        resp.set_cookie("player_id", pid, max_age=60 * 60 * 24 * 365)
        return resp

    current = get_current_participant(request)
    return render_template(
        "login.html",
        admin_enabled=admin_enabled(),
        is_admin=is_admin(),
        participants=active_participants,
        selected_pseudo=current["pseudo"] if current else None
    )


@app.route("/logout")
def logout():
    resp = make_response(redirect(url_for("home")))
    resp.delete_cookie("player_name")
    resp.delete_cookie("player_id")
    return resp


@app.route("/championnat", methods=["GET", "POST"])
def championnat():
    participant = get_current_participant(request)
    if not participant:
        flash("Choisis d’abord un pseudo autorisé.")
        return redirect(url_for("login"))

    name = participant["pseudo"]
    pid = request.cookies.get("player_id") or str(uuid.uuid4())
    player_norm = participant["pseudo_normalized"]

    state = get_championnat_state()

    my = {}
    if engine:
        with engine.begin() as conn:
            row = conn.execute(text("""
                SELECT payload_json, created_at, updated_at
                FROM championnat_pronos
                WHERE COALESCE(player_norm, LOWER(BTRIM(player_name)))=:pn
                ORDER BY updated_at DESC
                LIMIT 1
            """), {"pn": player_norm}).fetchone()
        if row:
            my = dict(row[0] or {})
            my["_created_at"] = str(row[1]) if row[1] else None
            my["_updated_at"] = str(row[2]) if row[2] else None
    else:
        all_preds = load_json(championnat_path(), {})
        best = None
        for _, p in (all_preds or {}).items():
            if normalize_pseudo(p.get("player_name")) == player_norm:
                if best is None or (_parse_dt_maybe(p.get("updated_at")) or datetime.min) > (_parse_dt_maybe(best.get("updated_at")) or datetime.min):
                    best = p
        if best:
            my = dict(best)

    if request.method == "POST":
        if not state.get("is_open", True):
            flash("Les pronostics championnat du monde sont fermés.")
            return redirect(url_for("championnat"))

        form = request.form
        picks = [form.get("wc_p1"), form.get("wc_p2"), form.get("wc_p3")]
        v = [x for x in picks if x]
        if len(set(v)) != len(v):
            flash("Doublon détecté : tu ne peux pas mettre le même pilote 2 fois.")
            return redirect(url_for("championnat"))

        payload = {
            "player_name": name,
            "wc_p1": form.get("wc_p1"),
            "wc_p2": form.get("wc_p2"),
            "wc_p3": form.get("wc_p3"),
        }

        if engine:
            with engine.begin() as conn:
                existing = conn.execute(text("""
                    SELECT id
                    FROM championnat_pronos
                    WHERE COALESCE(player_norm, LOWER(BTRIM(player_name)))=:pn
                    ORDER BY updated_at DESC
                    LIMIT 1
                """), {"pn": player_norm}).fetchone()

                if existing:
                    conn.execute(text("""
                        UPDATE championnat_pronos
                        SET user_key=:u,
                            player_name=:n,
                            player_norm=:pn,
                            participant_id=:pidb,
                            payload_json=CAST(:p AS jsonb),
                            updated_at=NOW()
                        WHERE id=:id
                    """), {
                        "u": pid,
                        "n": name,
                        "pn": player_norm,
                        "pidb": participant["id"],
                        "p": json.dumps(payload, ensure_ascii=False),
                        "id": existing[0]
                    })
                else:
                    conn.execute(text("""
                        INSERT INTO championnat_pronos (
                            user_key, player_name, player_norm, participant_id,
                            payload_json, created_at, updated_at
                        )
                        VALUES (
                            :u, :n, :pn, :pidb,
                            CAST(:p AS jsonb), NOW(), NOW()
                        )
                    """), {
                        "u": pid,
                        "n": name,
                        "pn": player_norm,
                        "pidb": participant["id"],
                        "p": json.dumps(payload, ensure_ascii=False)
                    })
        else:
            all_preds = load_json(championnat_path(), {})
            payload["updated_at"] = datetime.utcnow().isoformat()
            payload["player_name"] = name
            all_preds[player_norm] = payload
            save_json(championnat_path(), all_preds)

        flash("Pronostic championnat enregistré ✅")
        resp = make_response(redirect(url_for("championnat")))
        resp.set_cookie("player_name", name, max_age=60 * 60 * 24 * 365)
        resp.set_cookie("player_id", pid, max_age=60 * 60 * 24 * 365)
        return resp

    return render_template(
        "championnat.html",
        riders=RIDERS,
        my=my,
        name=name,
        championnat_state=state,
        admin_enabled=admin_enabled(),
        is_admin=is_admin()
    )


@app.route("/championnat/public")
def championnat_public():
    state = get_championnat_state()
    if not state.get("revealed", False):
        return "Les pronostics championnat ne sont pas encore divulgués.", 403

    pronos_list = get_latest_championnat_pronos_by_player()
    pronos_list.sort(
        key=lambda x: _parse_dt_maybe(x.get("updated_at")) or datetime.min,
        reverse=True
    )

    name, _ = current_player(request)
    return render_template(
        "championnat_public.html",
        name=name,
        pronos=pronos_list,
        championnat_state=state,
        admin_enabled=admin_enabled(),
        is_admin=is_admin()
    )


@app.route("/w/<weekend_id>/pronos", methods=["GET", "POST"])
def pronos(weekend_id):
    participant = get_current_participant(request)
    if not participant:
        flash("Choisis d’abord un pseudo autorisé.")
        return redirect(url_for("login"))

    name = participant["pseudo"]
    pid = request.cookies.get("player_id") or str(uuid.uuid4())
    player_norm = participant["pseudo_normalized"]

    w = get_weekend(weekend_id)
    if not w:
        return "Week-end inconnu", 404

    if w.get("cancelled"):
        return "Ce GP est annulé. Les pronostics sont indisponibles.", 403

    closed = is_weekend_closed(weekend_id) if engine else False

    if closed:
        if not engine:
            return "Mode public indisponible sans DB (DATABASE_URL manquant).", 500
        return redirect(url_for("public_pronos", weekend_id=weekend_id))

    my = {}
    if engine:
        with engine.begin() as conn:
            row = conn.execute(text("""
                SELECT payload_json, created_at, updated_at
                FROM pronos
                WHERE weekend_id=:w
                  AND COALESCE(player_norm, LOWER(BTRIM(player_name)))=:pn
                ORDER BY updated_at DESC
                LIMIT 1
            """), {"w": weekend_id, "pn": player_norm}).fetchone()
        if row:
            my = dict(row[0] or {})
            my["_created_at"] = str(row[1]) if row[1] else None
            my["_updated_at"] = str(row[2]) if row[2] else None
    else:
        all_pronos = load_json(pronos_path(weekend_id), {})
        best = None
        for _, p in (all_pronos or {}).items():
            if normalize_pseudo(p.get("player_name")) == player_norm:
                if best is None or (_parse_dt_maybe(p.get("updated_at")) or datetime.min) > (_parse_dt_maybe(best.get("updated_at")) or datetime.min):
                    best = p
        if best:
            my = dict(best)

    if request.method == "POST":
        if closed:
            flash("Pronos clos : modification impossible.")
            return redirect(url_for("pronos", weekend_id=weekend_id))

        form = request.form

        def has_duplicates(values):
            v = [x for x in values if x]
            return len(set(v)) != len(v)

        if has_duplicates([form.get("q1_1"), form.get("q1_2")]) \
           or has_duplicates([form.get("sprint_p1"), form.get("sprint_p2"), form.get("sprint_p3")]) \
           or has_duplicates([form.get("gp_p1"), form.get("gp_p2"), form.get("gp_p3")]):
            flash("Doublon détecté : un pilote ne peut apparaître qu'une fois dans Q1 / Sprint / GP.")
            return redirect(url_for("pronos", weekend_id=weekend_id))

        bonus_answers = {}
        for b in w.get("bonus_questions", []):
            bid = b["id"]
            answer = form.get(f"bonus_{bid}")
            ok, err = validate_bonus_answer_for_question(b, answer)
            if not ok:
                flash(err)
                return redirect(url_for("pronos", weekend_id=weekend_id))
            bonus_answers[bid] = "" if answer is None else str(answer).strip()

        payload = {
            "player_name": name,
            "pole": form.get("pole"),
            "q1_1": form.get("q1_1"),
            "q1_2": form.get("q1_2"),
            "sprint_p1": form.get("sprint_p1"),
            "sprint_p2": form.get("sprint_p2"),
            "sprint_p3": form.get("sprint_p3"),
            "gp_p1": form.get("gp_p1"),
            "gp_p2": form.get("gp_p2"),
            "gp_p3": form.get("gp_p3"),
            "bonus": bonus_answers
        }

        if engine:
            with engine.begin() as conn:
                existing = conn.execute(text("""
                    SELECT id
                    FROM pronos
                    WHERE weekend_id=:w
                      AND COALESCE(player_norm, LOWER(BTRIM(player_name)))=:pn
                    ORDER BY updated_at DESC
                    LIMIT 1
                """), {"w": weekend_id, "pn": player_norm}).fetchone()

                if existing:
                    conn.execute(text("""
                        UPDATE pronos
                        SET user_key=:u,
                            player_name=:n,
                            player_norm=:pn,
                            participant_id=:pidb,
                            payload_json=CAST(:p AS jsonb),
                            updated_at=NOW()
                        WHERE id=:id
                    """), {
                        "u": pid,
                        "n": name,
                        "pn": player_norm,
                        "pidb": participant["id"],
                        "p": json.dumps(payload, ensure_ascii=False),
                        "id": existing[0]
                    })
                else:
                    conn.execute(text("""
                        INSERT INTO pronos (
                            weekend_id, user_key, player_name, player_norm, participant_id,
                            payload_json, created_at, updated_at
                        )
                        VALUES (
                            :w, :u, :n, :pn, :pidb,
                            CAST(:p AS jsonb), NOW(), NOW()
                        )
                    """), {
                        "w": weekend_id,
                        "u": pid,
                        "n": name,
                        "pn": player_norm,
                        "pidb": participant["id"],
                        "p": json.dumps(payload, ensure_ascii=False)
                    })
        else:
            all_pronos = load_json(pronos_path(weekend_id), {})
            payload["updated_at"] = datetime.utcnow().isoformat()
            payload["player_name"] = name
            all_pronos[player_norm] = payload
            save_json(pronos_path(weekend_id), all_pronos)

        flash("Pronostic enregistré ✅ (modifiable à volonté)")
        resp = make_response(redirect(url_for("pronos", weekend_id=weekend_id)))
        resp.set_cookie("player_name", name, max_age=60 * 60 * 24 * 365)
        resp.set_cookie("player_id", pid, max_age=60 * 60 * 24 * 365)
        return resp

    return render_template(
        "pronos.html",
        w=w,
        riders=RIDERS,
        all_teams=ALL_TEAMS,
        official_teams=OFFICIAL_TEAMS,
        satellite_teams=SATELLITE_TEAMS,
        bonus_type_choices=BONUS_TYPE_CHOICES,
        my=my,
        name=name,
        closed=closed,
        admin_enabled=admin_enabled(),
        is_admin=is_admin()
    )


# ================== ADMIN AUTH ==================
@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if not admin_enabled():
        return "Admin non configuré (ADMIN_USER/ADMIN_PASS manquants).", 500

    if request.method == "POST":
        u = (request.form.get("username") or "").strip()
        p = (request.form.get("password") or "").strip()

        if u == ADMIN_USER and p == ADMIN_PASS:
            session["is_admin"] = True
            session.permanent = True
            flash("✅ Connecté en admin")
            return redirect(url_for("admin_home"))

        flash("❌ Identifiants incorrects")
        return redirect(url_for("admin_login"))

    name, _ = current_player(request)
    return render_template("admin_login.html", name=name)


@app.route("/admin/logout")
def admin_logout():
    session.pop("is_admin", None)
    flash("Déconnecté de l'admin ✅")
    return redirect(url_for("home"))


# ================== ADMIN PAGES ==================
@app.route("/admin")
@require_admin
def admin_home():
    weekends = get_sorted_weekends()
    enriched = []

    for w in weekends:
        wid = w.get("id")
        if not wid:
            continue

        count = count_distinct_pronos_for_weekend(wid)

        enriched.append({
            "id": wid,
            "label": w.get("label", wid),
            "date": w.get("date"),
            "count": count,
            "closed": is_weekend_closed(wid) if engine else False,
            "public": is_weekend_closed(wid) if engine else False,
            "bonus_count": len(w.get("bonus_questions", []) or []),
            "cancelled": bool(w.get("cancelled", False)),
        })

    championnat_count = len(get_latest_championnat_pronos_by_player())
    championnat_state = get_championnat_state()
    participants_count = len(get_all_participants(include_inactive=True))

    name, _ = current_player(request)
    return render_template(
        "admin_home.html",
        name=name,
        weekends=enriched,
        championnat_count=championnat_count,
        championnat_state=championnat_state,
        participants_count=participants_count
    )


@app.route("/admin/participants", methods=["GET", "POST"])
@require_admin
def admin_participants():
    if request.method == "POST":
        pseudo = (request.form.get("pseudo") or "").strip()
        secret = (request.form.get("secret") or "").strip()
        ok, msg = add_participant(pseudo, secret)
        flash(msg)
        return redirect(url_for("admin_participants"))

    participants = get_all_participants(include_inactive=True)
    name, _ = current_player(request)
    return render_template(
        "admin_participants.html",
        name=name,
        participants=participants
    )


@app.route("/admin/participants/<int:participant_id>/toggle", methods=["POST"])
@require_admin
def admin_toggle_participant(participant_id):
    ok, msg = toggle_participant(participant_id)
    flash(msg)
    return redirect(url_for("admin_participants"))


@app.route("/admin/participants/<int:participant_id>/reset_secret", methods=["POST"])
@require_admin
def admin_reset_participant_secret(participant_id):
    new_secret = (request.form.get("new_secret") or request.form.get("secret") or "").strip()
    ok, msg = reset_participant_secret(participant_id, new_secret)
    flash(msg)
    return redirect(url_for("admin_participants"))


@app.route("/admin/participants/<int:participant_id>/delete", methods=["POST"])
@require_admin
def admin_delete_participant(participant_id):
    ok, msg = delete_participant(participant_id)
    flash(msg)
    return redirect(url_for("admin_participants"))


@app.route("/admin/championnat")
@require_admin
def admin_championnat():
    state = get_championnat_state()
    pronos_list = get_latest_championnat_pronos_by_player()
    count = len(pronos_list)

    name, _ = current_player(request)
    return render_template(
        "admin_championnat.html",
        name=name,
        championnat_state=state,
        count=count,
        pronos=pronos_list
    )


@app.route("/admin/championnat/open", methods=["POST"])
@require_admin
def admin_open_championnat():
    open_championnat()
    flash("🟢 Pronostics championnat OUVERTS")
    return redirect(url_for("admin_championnat"))


@app.route("/admin/championnat/close", methods=["POST"])
@require_admin
def admin_close_championnat():
    close_championnat()
    flash("🔒 Pronostics championnat FERMÉS")
    return redirect(url_for("admin_championnat"))


@app.route("/admin/championnat/reveal", methods=["POST"])
@require_admin
def admin_reveal_championnat():
    state = get_championnat_state()
    if state.get("is_open", True):
        flash("Ferme d’abord les pronostics avant de les divulguer.")
        return redirect(url_for("admin_championnat"))

    reveal_championnat()
    flash("👁️ Pronostics championnat divulgués")
    return redirect(url_for("admin_championnat"))


@app.route("/admin/championnat/hide", methods=["POST"])
@require_admin
def admin_hide_championnat():
    hide_championnat()
    flash("🙈 Pronostics championnat masqués")
    return redirect(url_for("admin_championnat"))


@app.route("/admin/w/<weekend_id>")
@require_admin
def admin_weekend(weekend_id):
    w = get_weekend(weekend_id)
    if not w:
        return "Week-end inconnu", 404

    count = count_distinct_pronos_for_weekend(weekend_id)
    closed = is_weekend_closed(weekend_id) if engine else False
    public = closed

    name, _ = current_player(request)
    return render_template(
        "admin_weekend.html",
        name=name,
        w=w,
        count=count,
        closed=closed,
        public=public,
        cancelled=bool(w.get("cancelled", False)),
        bonus_type_choices=BONUS_TYPE_CHOICES,
        all_teams=ALL_TEAMS,
        official_teams=OFFICIAL_TEAMS,
        satellite_teams=SATELLITE_TEAMS
    )


@app.route("/admin/w/<weekend_id>/toggle_pronos", methods=["POST"])
@require_admin
def admin_toggle_pronos(weekend_id):
    if not get_weekend(weekend_id):
        return "Week-end inconnu", 404
    if not engine:
        return "DB non configurée (DATABASE_URL manquant).", 500

    state = (request.form.get("state") or "").strip().lower()
    if state == "on":
        set_weekend_open(weekend_id)
        flash("🟢 Pronos OUVERTS (et redevenus privés)")
    else:
        set_weekend_closed_and_public(weekend_id)
        flash("🔴 Pronos FERMÉS (et rendus publics)")

    return redirect(url_for("admin_weekend", weekend_id=weekend_id))


@app.route("/admin/calendar")
@require_admin
def admin_calendar():
    items = []
    for w in get_sorted_weekends():
        wid = w.get("id")
        closed = is_weekend_closed(wid) if engine else False
        cancelled = bool(w.get("cancelled", False))
        status = weekend_status(
            w.get("date_obj"),
            open_days_before=10,
            cancelled=cancelled
        )

        items.append({
            "id": wid,
            "label": w.get("label", wid),
            "date": w.get("date"),
            "date_obj": w.get("date_obj"),
            "closed": closed,
            "public": closed,
            "cancelled": cancelled,
            "status": status,
            "count": count_distinct_pronos_for_weekend(wid),
            "can_edit_date": not closed,
            "can_cancel": not closed and not cancelled,
            "can_reactivate": not closed and cancelled,
        })

    name, _ = current_player(request)
    return render_template(
        "admin_calendar.html",
        name=name,
        weekends=items
    )


@app.route("/admin/calendar/<weekend_id>/update", methods=["POST"])
@require_admin
def admin_calendar_update(weekend_id):
    w = get_weekend(weekend_id)
    if not w:
        return "Week-end inconnu", 404

    if is_weekend_closed(weekend_id):
        flash("GP déjà clos : date non modifiable.")
        return redirect(url_for("admin_calendar"))

    new_date = (request.form.get("date") or "").strip()
    new_label = (request.form.get("label") or "").strip()

    ok, msg = update_weekend_calendar(
        weekend_id=weekend_id,
        new_date=new_date,
        new_label=new_label
    )
    flash(msg)
    return redirect(url_for("admin_calendar"))


@app.route("/admin/calendar/<weekend_id>/cancel", methods=["POST"])
@require_admin
def admin_calendar_cancel(weekend_id):
    w = get_weekend(weekend_id)
    if not w:
        return "Week-end inconnu", 404

    if is_weekend_closed(weekend_id):
        flash("GP déjà clos : annulation impossible depuis l'admin calendrier.")
        return redirect(url_for("admin_calendar"))

    ok, msg = set_weekend_cancelled_flag(weekend_id, True)
    flash("GP marqué comme annulé ✅" if ok else msg)
    return redirect(url_for("admin_calendar"))


@app.route("/admin/calendar/<weekend_id>/reactivate", methods=["POST"])
@require_admin
def admin_calendar_reactivate(weekend_id):
    w = get_weekend(weekend_id)
    if not w:
        return "Week-end inconnu", 404

    if is_weekend_closed(weekend_id):
        flash("GP déjà clos : réactivation impossible.")
        return redirect(url_for("admin_calendar"))

    ok, msg = set_weekend_cancelled_flag(weekend_id, False)
    flash("GP réactivé ✅" if ok else msg)
    return redirect(url_for("admin_calendar"))


@app.route("/admin/w/<weekend_id>/results", methods=["GET", "POST"])
@require_admin
def admin_results(weekend_id):
    w = get_weekend(weekend_id)
    if not w:
        return "Week-end inconnu", 404

    if w.get("cancelled"):
        flash("GP annulé : saisie des résultats désactivée.")
        return redirect(url_for("admin_weekend", weekend_id=weekend_id))

    results = load_results(weekend_id) or {}
    results.setdefault("bonus", {})

    if request.method == "POST":
        f = request.form

        bonus_results = {}
        for b in w.get("bonus_questions", []):
            bid = b["id"]
            answer = f.get(f"bonus_{bid}")
            ok, err = validate_bonus_answer_for_question(b, answer)
            if not ok:
                flash(f"Bonne réponse bonus invalide : {err}")
                return redirect(url_for("admin_results", weekend_id=weekend_id))
            bonus_results[bid] = "" if answer is None else str(answer).strip()

        results = {
            "pole": f.get("pole"),
            "q1": [f.get("q1_1"), f.get("q1_2")],
            "sprint": [f.get("sprint_p1"), f.get("sprint_p2"), f.get("sprint_p3")],
            "gp": [f.get("gp_p1"), f.get("gp_p2"), f.get("gp_p3")],
            "bonus": bonus_results
        }
        save_results(weekend_id, results)

        data = load_weekends_data()
        for ww in data.get("weekends", []):
            if ww.get("id") == weekend_id:
                questions = sanitize_bonus_questions_list(ww.get("bonus_questions", []))
                for q in questions:
                    q["correct_answer"] = bonus_results.get(q["id"], "")
                ww["bonus_questions"] = questions
                break
        save_weekends_data(data)

        flash("Résultats officiels enregistrés ✅")
        return redirect(url_for("admin_results", weekend_id=weekend_id))

    name, _ = current_player(request)
    return render_template(
        "admin_results.html",
        name=name,
        w=w,
        riders=RIDERS,
        all_teams=ALL_TEAMS,
        official_teams=OFFICIAL_TEAMS,
        satellite_teams=SATELLITE_TEAMS,
        bonus_type_choices=BONUS_TYPE_CHOICES,
        results=results
    )


@app.route("/admin/w/<weekend_id>/questions", methods=["GET", "POST"])
@require_admin
def admin_questions(weekend_id):
    w = get_weekend(weekend_id)
    if not w:
        return "Week-end inconnu", 404

    if w.get("cancelled"):
        flash("GP annulé : gestion des questions bonus désactivée.")
        return redirect(url_for("admin_weekend", weekend_id=weekend_id))

    if request.method == "POST":
        action = (request.form.get("action") or "add").strip().lower()

        if action == "add":
            label = (request.form.get("label") or "").strip()
            qtype = (request.form.get("type") or "yes_no").strip()
            options_raw = (request.form.get("range_options") or "").strip()

            if not label:
                flash("Le texte de la question est obligatoire.")
                return redirect(url_for("admin_questions", weekend_id=weekend_id))

            ok = add_bonus_question_to_weekend(weekend_id, label, qtype, options_raw)
            flash("Question bonus ajoutée ✅" if ok else "Impossible d'ajouter la question bonus.")
            return redirect(url_for("admin_questions", weekend_id=weekend_id))

        if action == "update":
            question_id = (request.form.get("question_id") or "").strip()
            label = (request.form.get("label") or "").strip()
            qtype = (request.form.get("type") or "yes_no").strip()
            options_raw = (request.form.get("range_options") or "").strip()

            if not question_id:
                flash("Question introuvable.")
                return redirect(url_for("admin_questions", weekend_id=weekend_id))

            if not label:
                flash("Le texte de la question est obligatoire.")
                return redirect(url_for("admin_questions", weekend_id=weekend_id))

            ok = update_bonus_question_in_weekend(weekend_id, question_id, label, qtype, options_raw)
            flash("Question bonus modifiée ✅" if ok else "Impossible de modifier la question bonus.")
            return redirect(url_for("admin_questions", weekend_id=weekend_id))

        if action == "delete":
            question_id = (request.form.get("question_id") or "").strip()
            if not question_id:
                flash("Question introuvable.")
                return redirect(url_for("admin_questions", weekend_id=weekend_id))

            ok = delete_bonus_question_from_weekend(weekend_id, question_id)
            flash("Question bonus supprimée ✅" if ok else "Impossible de supprimer la question bonus.")
            return redirect(url_for("admin_questions", weekend_id=weekend_id))

        flash("Action inconnue.")
        return redirect(url_for("admin_questions", weekend_id=weekend_id))

    name, _ = current_player(request)
    try:
        return render_template(
            "admin_questions.html",
            name=name,
            w=w,
            bonus_type_choices=BONUS_TYPE_CHOICES,
            all_teams=ALL_TEAMS,
            official_teams=OFFICIAL_TEAMS,
            satellite_teams=SATELLITE_TEAMS
        )
    except Exception:
        return render_template(
            "admin_question.html",
            name=name,
            w=w,
            bonus_type_choices=BONUS_TYPE_CHOICES,
            all_teams=ALL_TEAMS,
            official_teams=OFFICIAL_TEAMS,
            satellite_teams=SATELLITE_TEAMS
        )


@app.route("/w/<weekend_id>/public/pronos")
def public_pronos(weekend_id):
    w = get_weekend(weekend_id)
    if not w:
        return "Week-end inconnu", 404
    if not engine:
        return "DB non configurée (DATABASE_URL manquant).", 500

    if w.get("cancelled"):
        return "Ce GP est annulé.", 403

    if not is_weekend_closed(weekend_id):
        return "Pronos pas encore visibles : le GP est encore ouvert.", 403

    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT ON (COALESCE(player_norm, LOWER(BTRIM(player_name))))
                player_name, payload_json, updated_at
            FROM pronos
            WHERE weekend_id=:w
            ORDER BY COALESCE(player_norm, LOWER(BTRIM(player_name))), updated_at DESC
        """), {"w": weekend_id}).fetchall()

    pronos_list = [{
        "player": r[0],
        "updated_at": str(r[2]) if r[2] else None,
        "p": dict(r[1] or {}),
    } for r in rows]

    pronos_list.sort(
        key=lambda x: _parse_dt_maybe(x.get("updated_at")) or datetime.min,
        reverse=True
    )

    name, _ = current_player(request)
    return render_template(
        "public_pronos.html",
        name=name,
        w=w,
        pronos=pronos_list,
        admin_enabled=admin_enabled(),
        is_admin=is_admin()
    )


@app.route("/results_by_race")
def results_by_race():
    name, _ = current_player(request)

    weekends = get_sorted_weekends()
    gp_id = (request.args.get("gp") or "").strip()

    selected = get_weekend(gp_id) if gp_id else None

    if not selected:
        return render_template(
            "results_by_race.html",
            name=name,
            weekends=weekends,
            selected=None,
            results=None,
            rows=[],
            notice=None,
            admin_enabled=admin_enabled(),
            is_admin=is_admin()
        )

    if selected.get("cancelled"):
        return render_template(
            "results_by_race.html",
            name=name,
            weekends=weekends,
            selected=selected,
            results=None,
            rows=[],
            notice="Ce GP est annulé.",
            admin_enabled=admin_enabled(),
            is_admin=is_admin()
        )

    results = load_results(gp_id)
    if not results:
        return render_template(
            "results_by_race.html",
            name=name,
            weekends=weekends,
            selected=selected,
            results=None,
            rows=[],
            notice="Entre d’abord les résultats officiels (Admin).",
            admin_enabled=admin_enabled(),
            is_admin=is_admin()
        )

    pronos_list = []
    if engine:
        with engine.begin() as conn:
            db_rows = conn.execute(text("""
                SELECT DISTINCT ON (COALESCE(player_norm, LOWER(BTRIM(player_name))))
                    player_name, payload_json, created_at, updated_at
                FROM pronos
                WHERE weekend_id=:w
                ORDER BY COALESCE(player_norm, LOWER(BTRIM(player_name))), updated_at DESC
            """), {"w": gp_id}).fetchall()

        for r in db_rows:
            pronos_list.append({
                "player_name": r[0],
                "payload": dict(r[1] or {}),
                "created_at": str(r[2]) if r[2] else None,
                "updated_at": str(r[3]) if r[3] else None,
            })
    else:
        all_pronos = load_json(pronos_path(gp_id), {})
        if isinstance(all_pronos, dict):
            tmp = []
            for _, p in all_pronos.items():
                tmp.append({
                    "player_name": p.get("player_name", "??"),
                    "payload": dict(p or {}),
                    "created_at": p.get("_created_at") or p.get("created_at"),
                    "updated_at": p.get("_updated_at") or p.get("updated_at"),
                })
            pronos_list = dedupe_pronos_by_playername(tmp)

    rows = []
    for item in pronos_list:
        p = item["payload"] or {}
        breakdown = compute_points_breakdown(p, results, selected)
        rows.append({
            "player": item["player_name"],
            "created_at": item.get("created_at"),
            "updated_at": item.get("updated_at"),
            "p": p,
            "pts": breakdown
        })

    rows.sort(key=lambda x: x["pts"]["total"], reverse=True)

    return render_template(
        "results_by_race.html",
        name=name,
        weekends=weekends,
        selected=selected,
        results=results,
        rows=rows,
        notice=None,
        admin_enabled=admin_enabled(),
        is_admin=is_admin()
    )


@app.route("/w/<weekend_id>/classement")
def classement_weekend(weekend_id):
    w = get_weekend(weekend_id)
    if not w:
        return "Week-end inconnu", 404

    if w.get("cancelled"):
        return "Ce GP est annulé.", 403

    closed = is_weekend_closed(weekend_id) if engine else False
    pronos_by_player = get_latest_pronos_by_player_for_weekend(weekend_id)

    results = load_results(weekend_id)
    if not results:
        name, _ = current_player(request)
        return render_template(
            "classement.html",
            name=name,
            w=w,
            rows=[],
            notice="Entre d’abord les résultats officiels (Admin).",
            closed=closed,
            admin_enabled=admin_enabled(),
            is_admin=is_admin()
        )

    rows = []
    for player_name, p in pronos_by_player.items():
        q_score = qualif_points(
            p.get("pole"),
            results.get("pole"),
            [p.get("q1_1"), p.get("q1_2")],
            results.get("q1", []),
        )

        s_score = podium_points(
            [p.get("sprint_p1"), p.get("sprint_p2"), p.get("sprint_p3")],
            results.get("sprint", []),
            well_placed=1.0,
            mis_placed=0.5,
            bonus_exact=3.0,
            bonus_all=1.5
        )

        g_score = podium_points(
            [p.get("gp_p1"), p.get("gp_p2"), p.get("gp_p3")],
            results.get("gp", []),
            well_placed=2.0,
            mis_placed=1.0,
            bonus_exact=6.0,
            bonus_all=3.0
        )

        bonus_score = 0.0
        for b in w.get("bonus_questions", []):
            bid = b.get("id")
            if not bid:
                continue
            pred = (p.get("bonus", {}) or {}).get(bid)
            real = (results.get("bonus", {}) or {}).get(bid)
            if is_bonus_answer_correct(b, pred, real):
                bonus_score += BONUS_POINTS_PER_QUESTION

        total = round(q_score + s_score + g_score + bonus_score, 2)
        rows.append({
            "player": player_name or p.get("player_name", "??"),
            "q": round(q_score, 2),
            "s": round(s_score, 2),
            "gp": round(g_score, 2),
            "bonus": round(bonus_score, 2),
            "total": round(total, 2)
        })

    rows.sort(key=lambda r: r["total"], reverse=True)

    name, _ = current_player(request)
    return render_template(
        "classement.html",
        name=name,
        w=w,
        rows=rows,
        notice=None,
        closed=closed,
        admin_enabled=admin_enabled(),
        is_admin=is_admin()
    )


@app.route("/classement")
def classement_general():
    name, _ = current_player(request)

    weekends = get_sorted_weekends()
    if not weekends:
        return render_template(
            "classement_general.html",
            name=name,
            rows=[],
            gp_scored=[],
            notice="Aucun week-end configuré.",
            admin_enabled=admin_enabled(),
            is_admin=is_admin()
        )

    totals = {}
    gp_scored = []

    for w in weekends:
        if w.get("cancelled"):
            continue

        wid = (w.get("id") or "").strip()
        if not wid:
            continue

        results = load_results(wid)
        if not results:
            continue

        gp_scored.append({"id": wid, "label": w.get("label", wid)})

        pronos_by_player = get_latest_pronos_by_player_for_weekend(wid)

        for player_name, p in pronos_by_player.items():
            q_score = qualif_points(
                p.get("pole"),
                results.get("pole"),
                [p.get("q1_1"), p.get("q1_2")],
                results.get("q1", []),
            )

            s_score = podium_points(
                [p.get("sprint_p1"), p.get("sprint_p2"), p.get("sprint_p3")],
                results.get("sprint", []),
                well_placed=1.0,
                mis_placed=0.5,
                bonus_exact=3.0,
                bonus_all=1.5
            )

            g_score = podium_points(
                [p.get("gp_p1"), p.get("gp_p2"), p.get("gp_p3")],
                results.get("gp", []),
                well_placed=2.0,
                mis_placed=1.0,
                bonus_exact=6.0,
                bonus_all=3.0
            )

            bonus_score = 0.0
            for b in (w.get("bonus_questions", []) or []):
                bid = b.get("id")
                if not bid:
                    continue
                pred = (p.get("bonus", {}) or {}).get(bid)
                real = (results.get("bonus", {}) or {}).get(bid)
                if is_bonus_answer_correct(b, pred, real):
                    bonus_score += BONUS_POINTS_PER_QUESTION

            total_gp = round(q_score + s_score + g_score + bonus_score, 2)

            cur = totals.get(player_name)
            if not cur:
                totals[player_name] = {
                    "player": player_name,
                    "q": 0.0,
                    "s": 0.0,
                    "gp": 0.0,
                    "bonus": 0.0,
                    "total": 0.0,
                    "gps": 0
                }
                cur = totals[player_name]

            cur["q"] = round(cur["q"] + q_score, 2)
            cur["s"] = round(cur["s"] + s_score, 2)
            cur["gp"] = round(cur["gp"] + g_score, 2)
            cur["bonus"] = round(cur["bonus"] + bonus_score, 2)
            cur["total"] = round(cur["total"] + total_gp, 2)
            cur["gps"] += 1

    rows = list(totals.values())
    rows.sort(key=lambda r: (r["total"], r["gps"]), reverse=True)

    notice = None
    if not gp_scored:
        notice = "Aucun résultat saisi pour le moment (Admin → Entrer résultats)."

    return render_template(
        "classement_general.html",
        name=name,
        rows=rows,
        gp_scored=gp_scored,
        notice=notice,
        admin_enabled=admin_enabled(),
        is_admin=is_admin()
    )


# ------------------ Health checks ------------------
@app.route("/healthz")
def healthz():
    resp = make_response("OK", 200)
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.route("/health")
def health():
    if engine:
        try:
            ensure_db_bootstrap()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except Exception as e:
            return f"DB ERROR: {e}", 500

    resp = make_response("OK", 200)
    resp.headers["Cache-Control"] = "no-store"
    return resp


if __name__ == "__main__":
    app.run(debug=True)
