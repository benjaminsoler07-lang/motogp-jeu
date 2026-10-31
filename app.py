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
# + NOUVEAU : historique automatique des sauvegardes pronos (pronos_history)
# + NOUVEAU : journal admin simple des sauvegardes par GP

import os
import json
import shutil
import uuid
import secrets
import hmac
import re
import unicodedata
from datetime import datetime, date, timedelta, timezone
from functools import wraps
from threading import Lock

from flask import (
    Flask, render_template, render_template_string,
    request, redirect, url_for,
    make_response, flash, session, abort
)
from sqlalchemy import create_engine, text
from werkzeug.security import generate_password_hash, check_password_hash
from v2_features import register_v2, race_center, deadline_from_weekend
from v2_notifications import register_notifications
from v2_bulk_import import register_bulk_import
from gp_insights import build_public_insights, build_championship_insights


app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")
app.permanent_session_lifetime = timedelta(days=365)

ADMIN_USER = os.environ.get("ADMIN_USER", "")
ADMIN_PASS = os.environ.get("ADMIN_PASS", "")

BONUS_POINTS_PER_QUESTION = 0.5

# Vercel n'autorise pas les écritures dans /var/task ; seul /tmp est
# accessible en écriture. PostgreSQL reste la source des données persistantes.
IS_VERCEL = bool(os.environ.get("VERCEL"))
DATA_DIR = os.path.join("/tmp", "pronogp-data") if IS_VERCEL else "data"
PRONOS_DIR = os.path.join(DATA_DIR, "pronos")
RESULTS_DIR = os.path.join(DATA_DIR, "results")
PARTICIPANTS_FILE = os.path.join(DATA_DIR, "participants.json")
WEEKENDS_FILE = os.path.join(DATA_DIR, "weekends.json")

os.makedirs(PRONOS_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)

# Conserver le calendrier livré avec le code comme base de lecture sur
# Vercel sans écrire dans le système de fichiers de l'application.
if IS_VERCEL and not os.path.exists(WEEKENDS_FILE):
    bundled_weekends = os.path.join(os.path.dirname(__file__), "data", "weekends.json")
    if os.path.isfile(bundled_weekends):
        shutil.copyfile(bundled_weekends, WEEKENDS_FILE)

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


def is_root_admin():
    """Session principale préexistante (cookie Flask signé par le serveur)."""
    return session.get("is_admin") is True


def is_admin():
    """Droits délégués : revérification en base à chaque requête."""
    if is_root_admin():
        return True
    if not engine:
        return False
    participant = get_current_participant(request)
    return bool(participant and participant.get("is_admin") and participant.get("active"))


@app.context_processor
def inject_admin_access():
    return {"viewer_is_admin": is_admin(), "viewer_is_root_admin": is_root_admin()}


def require_admin(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not admin_enabled() and not is_admin():
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


def format_dt_fr(dt_value):
    dt = _parse_dt_maybe(dt_value) if not isinstance(dt_value, datetime) else dt_value
    if not dt:
        return ""
    try:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone().strftime("%d/%m/%Y à %H:%M:%S")
    except Exception:
        return str(dt_value)


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
    # Tolère ponctuation, tirets, apostrophes et espaces sans accepter d'autres mots.
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


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
        return list(question["resolved_options"]) if "resolved_options" in question else list(RIDERS)
    if qtype in {"team", "official_team", "satellite_team"}:
        if "resolved_options" in question:
            return list(question["resolved_options"])
        return list({"team": ALL_TEAMS, "official_team": OFFICIAL_TEAMS,
                     "satellite_team": SATELLITE_TEAMS}[qtype])
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


def load_bonus_reviews(weekend_id: str) -> dict:
    """Lit une seule fois les corrections du GP pour éviter les requêtes par joueur."""
    if not engine:
        return {}
    ensure_db_bootstrap()
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT player_norm, question_id, decision, prediction_snapshot,
                   official_snapshot, reviewed_by, reviewed_at
            FROM bonus_reviews WHERE weekend_id=:wid
        """), {"wid": weekend_id}).mappings().all()
    return {(r["player_norm"], r["question_id"]): dict(r) for r in rows}


def evaluated_bonus_answer(question, predicted, actual, player_name=None, reviews=None):
    """Correction humaine prioritaire, uniquement si les deux réponses n'ont pas changé."""
    automatic = is_bonus_answer_correct(question, predicted, actual)
    key = (normalize_pseudo(player_name), str(question.get("id") or ""))
    review = (reviews or {}).get(key)
    if review is not None and (
        str(review["prediction_snapshot"]) == str(predicted or "")
        and str(review["official_snapshot"]) == str(actual or "")
    ):
        return bool(review["decision"]), True
    return automatic, False


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


def weekend_season(w: dict) -> int:
    try:
        year = int((w or {}).get("season_year") or get_season_year())
        return year if 2020 <= year <= 2100 else get_season_year()
    except (ValueError, TypeError):
        return get_season_year()


def selected_season_year() -> int:
    """La saison courante évolue sans reclasser les anciens GP de 2026."""
    chosen = request.values.get("season")
    if chosen:
        try:
            year = int(chosen)
            return year if 2020 <= year <= 2100 else get_season_year()
        except (ValueError, TypeError):
            return get_season_year()
    legacy_year = get_season_year()
    if engine:
        ensure_db_bootstrap()
        with engine.connect() as conn:
            current = conn.execute(text("""
                SELECT MAX(year) FROM seasons
                WHERE archived=FALSE AND year<=:today
            """), {"today": date.today().year}).scalar()
        if current:
            return max(int(current), legacy_year)
    return legacy_year


def is_season_archived(year: int) -> bool:
    if not engine:
        return False
    ensure_db_bootstrap()
    with engine.connect() as conn:
        row = conn.execute(text("SELECT archived FROM seasons WHERE year=:y"), {"y": year}).fetchone()
    return bool(row and row[0])


def get_riders_for_season(year: int, *, active_only: bool = True) -> list:
    """Catalogue persistant par saison ; le barème et les choix archivés ne bougent pas."""
    if not engine:
        return list(RIDERS)
    ensure_db_bootstrap()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT rider FROM season_riders
            WHERE season_year=:year AND removed=FALSE
              AND (:active_only=FALSE OR active=TRUE)
            ORDER BY rider
        """), {"year": int(year), "active_only": bool(active_only)}).fetchall()
    return [r[0] for r in rows]


def get_teams_for_season(year: int, *, kind: str = None, active_only: bool = True) -> list:
    """Liste officielle/satellite persistante de la saison choisie."""
    if not engine:
        return list(ALL_TEAMS if not kind else
                    (OFFICIAL_TEAMS if kind == "official" else SATELLITE_TEAMS))
    ensure_db_bootstrap()
    where_kind = "AND kind=:kind" if kind in {"official", "satellite"} else ""
    with engine.connect() as conn:
        rows=conn.execute(text("""
            SELECT team FROM season_teams WHERE season_year=:year
        """ + where_kind + """
            AND removed=FALSE
            AND (:active_only=FALSE OR active=TRUE) ORDER BY team
        """), {"year":int(year),"kind":kind,"active_only":bool(active_only)}).fetchall()
    return [r[0] for r in rows]


def weekend_sort_key(w: dict):
    d = parse_weekend_date(w.get("date", ""), weekend_season(w))
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
            ALTER TABLE participants
            ADD COLUMN IF NOT EXISTS is_admin BOOLEAN NOT NULL DEFAULT FALSE;
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
        CREATE TABLE IF NOT EXISTS pronos_history (
            id SERIAL PRIMARY KEY,
            weekend_id TEXT NOT NULL,
            user_key TEXT NULL,
            player_name TEXT NOT NULL,
            player_norm TEXT NULL,
            participant_id INTEGER NULL,
            payload_json JSONB NOT NULL,
            source TEXT NOT NULL DEFAULT 'save',
            user_agent TEXT NULL,
            saved_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """))

        conn.execute(text("""
            ALTER TABLE pronos_history
            ADD COLUMN IF NOT EXISTS player_norm TEXT NULL;
        """))
        conn.execute(text("""
            ALTER TABLE pronos_history
            ADD COLUMN IF NOT EXISTS participant_id INTEGER NULL;
        """))
        conn.execute(text("""
            ALTER TABLE pronos_history
            ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'save';
        """))
        conn.execute(text("""
            ALTER TABLE pronos_history
            ADD COLUMN IF NOT EXISTS user_agent TEXT NULL;
        """))

        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_pronos_history_weekend_saved_at
            ON pronos_history(weekend_id, saved_at DESC);
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_pronos_history_weekend_player_norm
            ON pronos_history(weekend_id, player_norm, saved_at DESC);
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_pronos_history_participant_id
            ON pronos_history(participant_id);
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS seasons (
            year INTEGER PRIMARY KEY,
            archived BOOLEAN NOT NULL DEFAULT FALSE
        );
        """))
        conn.execute(text("""
            INSERT INTO seasons (year, archived) VALUES (2026, FALSE)
            ON CONFLICT (year) DO NOTHING
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS season_riders (
            season_year INTEGER NOT NULL REFERENCES seasons(year),
            rider TEXT NOT NULL,
            active BOOLEAN NOT NULL DEFAULT TRUE,
            PRIMARY KEY (season_year, rider)
        );
        """))
        conn.execute(text("""
            ALTER TABLE season_riders
            ADD COLUMN IF NOT EXISTS removed BOOLEAN NOT NULL DEFAULT FALSE;
        """))
        for rider in RIDERS:
            conn.execute(text("""
                INSERT INTO season_riders (season_year, rider, active)
                VALUES (2026, :rider, TRUE)
                ON CONFLICT (season_year, rider) DO NOTHING
            """), {"rider": rider})

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS season_teams (
            season_year INTEGER NOT NULL REFERENCES seasons(year),
            team TEXT NOT NULL,
            kind TEXT NOT NULL CHECK (kind IN ('official','satellite')),
            active BOOLEAN NOT NULL DEFAULT TRUE,
            PRIMARY KEY (season_year, team)
        );
        """))
        conn.execute(text("""
            ALTER TABLE season_teams
            ADD COLUMN IF NOT EXISTS removed BOOLEAN NOT NULL DEFAULT FALSE;
        """))
        for team in ALL_TEAMS:
            conn.execute(text("""
                INSERT INTO season_teams(season_year,team,kind,active)
                VALUES (2026,:team,:kind,TRUE)
                ON CONFLICT(season_year,team) DO NOTHING
            """), {"team":team, "kind":"official" if team in OFFICIAL_TEAMS else "satellite"})

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS web_push_subscriptions (
            endpoint TEXT PRIMARY KEY,
            participant_id INTEGER NOT NULL REFERENCES participants(id) ON DELETE CASCADE,
            p256dh TEXT NOT NULL,
            auth TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS push_messages (
            id BIGSERIAL PRIMARY KEY,
            participant_id INTEGER NOT NULL REFERENCES participants(id) ON DELETE CASCADE,
            category TEXT NOT NULL,
            title TEXT NOT NULL,
            body TEXT NOT NULL,
            destination TEXT NOT NULL,
            dedupe_key TEXT NOT NULL UNIQUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            sent_at TIMESTAMPTZ NULL,
            read_at TIMESTAMPTZ NULL
        );
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_push_messages_user
            ON push_messages (participant_id, created_at DESC);
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS player_photos (
            participant_id INTEGER PRIMARY KEY REFERENCES participants(id) ON DELETE CASCADE,
            photo_data BYTEA NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
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
        ALTER TABLE championnat_pronos
            ADD COLUMN IF NOT EXISTS season_year INTEGER NOT NULL DEFAULT 2026;
        """))
        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_championnat_pronos_season_year ON championnat_pronos(season_year);
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

        # Migration additive : l'historique des pronostics et résultats est préservé.
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS bonus_reviews (
            weekend_id TEXT NOT NULL,
            player_norm TEXT NOT NULL,
            question_id TEXT NOT NULL,
            decision BOOLEAN NOT NULL,
            prediction_snapshot TEXT NOT NULL,
            official_snapshot TEXT NOT NULL,
            reviewed_by TEXT NOT NULL,
            reviewed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (weekend_id, player_norm, question_id)
        );
        """))

        # Supabase crée les rôles anon/authenticated, PostgreSQL standard non.
        # Ne retirer des privilèges que pour les rôles qui existent réellement.
        api_roles = conn.execute(text("""
            SELECT rolname FROM pg_roles
            WHERE rolname IN ('anon', 'authenticated')
        """)).scalars().all()
        if api_roles:
            for table_name in (
                "bonus_reviews", "player_photos", "push_messages",
                "web_push_subscriptions", "seasons", "season_riders", "season_teams"
            ):
                # Table names and role names originate from fixed allow-lists.
                conn.execute(text(
                    "REVOKE ALL PRIVILEGES ON TABLE public." + table_name +
                    " FROM " + ", ".join(api_roles)
                ))

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

        conn.execute(text("""
            UPDATE pronos_history
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

    file_payload = load_json(
        WEEKENDS_FILE,
        {"season_year": date.today().year, "timezone": "Europe/Paris", "weekends": []}
    )
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
                qtype = q.get("type")
                if qtype == "rider":
                    q["resolved_options"] = get_riders_for_season(weekend_season(w))
                elif qtype in {"team", "official_team", "satellite_team"}:
                    kind = {"official_team":"official","satellite_team":"satellite"}.get(qtype)
                    q["resolved_options"] = get_teams_for_season(weekend_season(w), kind=kind)
                else:
                    q["resolved_options"] = get_bonus_options_for_type(qtype, q)
            return w
    return None


def get_sorted_weekends():
    items = []
    for w in load_weekends_list():
        ww = dict(w)
        ww["cancelled"] = bool(ww.get("cancelled", False))
        ww["date_obj"] = parse_weekend_date(ww.get("date", ""), weekend_season(ww))
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


def weekend_config_revision(weekend: dict) -> str:
    """Version courte du calendrier et des heures pour éviter les écrasements concurrents."""
    import hashlib
    relevant = {key: (weekend or {}).get(key) for key in (
        "date", "label", "sessions", "deadline_utc", "deadline_source", "auto_publish", "cancelled"
    )}
    payload = json.dumps(relevant, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def update_weekend_config_fields(weekend_id: str, updates: dict, expected_revision=None):
    """Met à jour UN GP atomiquement en PostgreSQL, sans toucher au disque de Render.

    Les résultats, pronos, indicateurs closed_at/public et autres GP ne changent pas.
    """
    allowed = {"date", "label", "deadline_utc", "deadline_source", "sessions", "auto_publish", "cancelled"}
    if not updates or set(updates) - allowed:
        return False, "Champs de calendrier invalides."
    data = load_weekends_data()
    base = next((dict(w) for w in data.get("weekends", [])
                 if w.get("id") == weekend_id), None)
    if base is None:
        return False, "Grand Prix introuvable."
    if engine:
        ensure_db_bootstrap()
        with engine.begin() as conn:
            row = conn.execute(text("""
                SELECT config_json FROM weekends WHERE weekend_id=:id FOR UPDATE
            """), {"id": weekend_id}).fetchone()
            if row is not None and row[0]:
                base.update(dict(row[0]))
            base["id"] = weekend_id
            if expected_revision and weekend_config_revision(base) != expected_revision:
                return False, "Le GP a été modifié depuis l'aperçu. Recharge les horaires."
            base.update(updates)
            conn.execute(text("""
                INSERT INTO weekends(weekend_id,config_json,updated_at)
                VALUES (:id,CAST(:config AS jsonb),NOW())
                ON CONFLICT (weekend_id) DO UPDATE
                SET config_json=EXCLUDED.config_json,updated_at=NOW()
            """), {
                "id": weekend_id,
                "config": json.dumps(base, ensure_ascii=False)
            })
    else:
        if expected_revision and weekend_config_revision(base) != expected_revision:
            return False, "Le GP a été modifié depuis l'aperçu. Recharge les horaires."
        for item in data.get("weekends", []):
            if item.get("id") == weekend_id:
                item.update(updates)
                break
        save_weekends_data(data)
    return True, "Configuration du GP enregistrée."


def update_weekend_configs_bulk(plans: list):
    """Enregistre une sélection de GP en UNE transaction, ou aucun si erreur.

    Le snapshot des horaires et les révisions de TOUS les GP sélectionnés
    sont revalidés sous verrou. Résultats, pronostics et drapeaux de publication
    sont totalement indépendants de config_json.
    """
    if not engine:
        return False, "Base PostgreSQL indisponible. Aucun GP modifié."
    if not isinstance(plans, list) or not 1 <= len(plans) <= 50:
        return False, "Sélection de GP invalide."
    allowed = {"date", "sessions", "deadline_utc", "deadline_source"}
    known = {
        w.get("id"): dict(w)
        for w in load_weekends_data().get("weekends", [])
        if w.get("id")
    }
    visited = set()
    for plan in plans:
        if not isinstance(plan, dict):
            return False, "Données d'import invalides."
        wid = plan.get("id")
        updates = plan.get("updates")
        if (not isinstance(wid, str) or wid in visited or wid not in known
                or not isinstance(updates, dict) or not updates
                or set(updates) - allowed
                or not isinstance(plan.get("revision"), str)
                or len(plan["revision"]) != 64):
            return False, "Sélection, champs ou version de GP invalides."
        if "sessions" in updates and (
            not isinstance(updates["sessions"], list)
            or not 1 <= len(updates["sessions"]) <= 20
        ):
            return False, "Liste de séances incorrecte."
        visited.add(wid)

    now = datetime.now(timezone.utc)
    try:
        with engine.begin() as conn:
            updates_ready = []
            for plan in sorted(plans, key=lambda p: p["id"]):
                wid = plan["id"]
                current = dict(known[wid])
                row = conn.execute(text("""
                    SELECT config_json, closed_at
                    FROM weekends WHERE weekend_id=:id
                    FOR UPDATE
                """), {"id": wid}).fetchone()
                if row is not None and row[0]:
                    current.update(dict(row[0]))
                current["id"] = wid
                if weekend_config_revision(current) != plan["revision"]:
                    raise ValueError(
                        "Le GP " + wid + " a changé depuis l'aperçu. Recharge l'import."
                    )
                changes = plan["updates"]
                old_deadline = deadline_from_weekend(current)
                already_closed = bool(row[1]) if row is not None else False
                if "date" in changes and already_closed:
                    raise ValueError(
                        "Le GP " + wid + " est clôturé : sa date ne peut pas "
                        "être changée par import global."
                    )
                if "date" in changes:
                    parsed = parse_weekend_date(changes["date"], weekend_season(current))
                    if not parsed or parsed.year != weekend_season(current):
                        raise ValueError("Date hors saison pour " + wid + ".")
                if "deadline_utc" in changes:
                    value = changes["deadline_utc"]
                    new_deadline = _parse_dt_maybe(value)
                    if (
                        already_closed or (old_deadline and old_deadline <= now)
                        or not new_deadline or new_deadline.tzinfo is None
                        or new_deadline.astimezone(timezone.utc) <= now
                    ):
                        raise ValueError(
                            "Clôture déjà passée ou GP fermé : import Q1 refusé pour " + wid
                        )
                current.update(changes)
                updates_ready.append((wid, current))

            for wid, config in updates_ready:
                conn.execute(text("""
                    INSERT INTO weekends (weekend_id,config_json,updated_at)
                    VALUES (:id,CAST(:cfg AS jsonb),NOW())
                    ON CONFLICT (weekend_id) DO UPDATE
                    SET config_json=EXCLUDED.config_json,updated_at=NOW()
                """), {
                    "id": wid,
                    "cfg": json.dumps(config, ensure_ascii=False)
                })
    except ValueError as exc:
        return False, str(exc)
    return True, str(len(updates_ready)) + " GP mis à jour en une transaction."


def update_weekend_calendar(weekend_id: str, new_date: str = None, new_label: str = None):
    gp = get_weekend(weekend_id)
    if not gp:
        return False, "Grand Prix introuvable."
    changes = {}
    if new_label is not None:
        changes["label"] = (new_label or "").strip() or gp.get("label", weekend_id)
    if new_date is not None:
        parsed = parse_weekend_date((new_date or "").strip(), weekend_season(gp))
        if not parsed or parsed.year != weekend_season(gp):
            return False, "Date invalide : format AAAA-MM-JJ et année de saison obligatoires."
        changes["date"] = parsed.isoformat()
    if not changes:
        return False, "Aucune modification."
    return update_weekend_config_fields(weekend_id, changes)


def set_weekend_cancelled_flag(weekend_id: str, cancelled: bool):
    return update_weekend_config_fields(weekend_id, {"cancelled": bool(cancelled)})


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
                    SELECT id, pseudo, pseudo_normalized, secret_hash, active, created_at, is_admin
                    FROM participants
                    ORDER BY pseudo ASC
                """)).fetchall()
            else:
                rows = conn.execute(text("""
                    SELECT id, pseudo, pseudo_normalized, secret_hash, active, created_at, is_admin
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
            "created_at": str(r[5]) if r[5] else None,
            "is_admin": bool(r[6])
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
                "is_admin": bool(p.get("is_admin", False)),
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
                    SELECT id, pseudo, pseudo_normalized, secret_hash, active, created_at, is_admin
                    FROM participants
                    WHERE pseudo_normalized=:pn AND active=TRUE
                    LIMIT 1
                """), {"pn": pseudo_norm}).fetchone()
            else:
                row = conn.execute(text("""
                    SELECT id, pseudo, pseudo_normalized, secret_hash, active, created_at, is_admin
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
            "created_at": str(row[5]) if row[5] else None,
            "is_admin": bool(row[6])
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


def valid_new_participant_secret(secret: str):
    """Minimum for NEW secrets; previously created shorter secrets still work for login."""
    if len(secret) < 8 or len(secret) > 100 or not secret.strip():
        return False, "Le nouveau mot de passe doit contenir entre 8 et 100 caractères."
    return True, ""


def participant_secret_marker(participant: dict) -> str:
    """Signed-session version: HMAC of hash, not the password or password hash itself."""
    stored = (participant or {}).get("secret_hash") or ""
    if not stored:
        return ""
    return hmac.new(
        app.secret_key.encode("utf-8"), stored.encode("utf-8"), "sha256"
    ).hexdigest()


def add_participant(pseudo: str, secret: str):
    pseudo = (pseudo or "").strip()
    secret = (secret or "").strip()
    pseudo_norm = normalize_pseudo(pseudo)

    if not pseudo or not pseudo_norm:
        return False, "Le pseudo est obligatoire."

    valid, reason = valid_new_participant_secret(secret)
    if not valid:
        return False, reason

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
        "is_admin": False,
        "created_at": datetime.utcnow().isoformat()
    })
    save_participants_fallback(items)
    return True, "Joueur ajouté."


def set_participant_admin_role(participant_id: int, enabled: bool):
    """La route appelante vérifie les droits admin PRINCIPAUX."""
    enabled = bool(enabled)
    if engine:
        ensure_db_bootstrap()
        with engine.begin() as conn:
            row = conn.execute(text("""
                SELECT pseudo, active, is_admin
                FROM participants WHERE id=:pid FOR UPDATE
            """), {"pid": participant_id}).fetchone()
            if not row:
                return False, "Joueur introuvable."
            if enabled and not row[1]:
                return False, "Réactive d'abord ce joueur avant de lui donner les droits admin."
            conn.execute(text("""
                UPDATE participants SET is_admin=:enabled WHERE id=:pid
            """), {"pid": participant_id, "enabled": enabled})
        return True, (
            "Droits administrateur activés pour " if enabled
            else "Droits administrateur retirés à "
        ) + str(row[0]) + "."

    items = load_participants_fallback()
    for p in items:
        if str(p.get("id")) == str(participant_id):
            if enabled and not p.get("active", True):
                return False, "Réactive d'abord ce joueur avant de lui donner les droits admin."
            p["is_admin"] = enabled
            save_participants_fallback(items)
            return True, ("Droits administrateur activés." if enabled
                          else "Droits administrateur retirés.")
    return False, "Joueur introuvable."


def reset_participant_secret(participant_id: int, new_secret: str):
    """Admin-only caller: no old password needed. Never return or log plaintext."""
    new_secret = (new_secret or "").strip()
    valid, reason = valid_new_participant_secret(new_secret)
    if not valid:
        return False, reason

    secret_hash = generate_password_hash(new_secret)

    if engine:
        ensure_db_bootstrap()
        # Mise à jour atomique : ne jamais annoncer une réussite si aucune ligne
        # n'a réellement été modifiée (notamment si le joueur a été supprimé).
        with engine.begin() as conn:
            row = conn.execute(text("""
                UPDATE participants
                SET secret_hash=:sh
                WHERE id=:pid
                RETURNING pseudo
            """), {"sh": secret_hash, "pid": participant_id}).fetchone()

        if not row:
            return False, "Joueur introuvable."
        return True, f"Mot de passe mis à jour pour {row[0]}."

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
    return True, f"Mot de passe mis à jour pour {found.get('pseudo', '??')}."


def change_participant_secret(participant: dict, current_secret: str, new_secret: str):
    """Require the old secret; atomic compare-and-update prevents racing an admin reset."""
    if not verify_participant_secret(participant, current_secret):
        return False, "Mot de passe actuel incorrect."

    new_secret = (new_secret or "").strip()
    valid, reason = valid_new_participant_secret(new_secret)
    if not valid:
        return False, reason
    if check_password_hash(participant["secret_hash"], new_secret):
        return False, "Choisis un mot de passe différent de l'actuel."

    new_hash = generate_password_hash(new_secret)
    if engine:
        ensure_db_bootstrap()
        with engine.begin() as conn:
            result = conn.execute(text("""
                UPDATE participants SET secret_hash=:new_hash
                WHERE id=:pid AND secret_hash=:old_hash AND active=TRUE
            """), {
                "new_hash": new_hash, "old_hash": participant["secret_hash"],
                "pid": participant["id"]
            })
        if result.rowcount != 1:
            return False, "Le mot de passe a changé entre-temps. Reconnecte-toi."
    else:
        items = load_participants_fallback()
        matched = False
        for row in items:
            if (str(row.get("id")) == str(participant["id"])
                    and row.get("secret_hash") == participant["secret_hash"]
                    and row.get("active", True)):
                row["secret_hash"] = new_hash
                matched = True
                break
        if not matched:
            return False, "Le mot de passe a changé entre-temps. Reconnecte-toi."
        save_participants_fallback(items)

    return True, "Mot de passe modifié. Les autres appareils devront se reconnecter."


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
                DELETE FROM bonus_reviews WHERE player_norm=:pn
            """), {"pn": pseudo_norm})
            conn.execute(text("""
                DELETE FROM pronos_history
                WHERE participant_id=:pid
                   OR COALESCE(player_norm, LOWER(BTRIM(player_name)))=:pn
            """), {"pid": participant_id, "pn": pseudo_norm})

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
    """Le cookie pseudo historique ne constitue jamais une authentification."""
    player_norm = session.get("authenticated_player")
    player_id = session.get("authenticated_player_id")
    if not player_norm or player_id is None:
        return None
    participant = get_participant_by_input(player_norm, active_only=True)
    expected_marker = participant_secret_marker(participant)
    if (not participant or str(participant["id"]) != str(player_id)
            or not expected_marker
            or not hmac.compare_digest(
                str(session.get("authenticated_secret_marker") or ""), expected_marker
            )):
        session.pop("authenticated_player", None)
        session.pop("authenticated_player_id", None)
        session.pop("authenticated_secret_marker", None)
        return None
    return participant


# ------------------ Accès privé au jeu ------------------
# Les classements, résultats et pronostics publiés sont visibles uniquement
# par les joueurs authentifiés ou l'administrateur.
# Les endpoints techniques restent disponibles pour le fonctionnement PWA,
# les contrôles de disponibilité et le dispatch GitHub (authentifié par OIDC).
PUBLIC_ENDPOINTS = frozenset({
    "login",
    "admin_login",
    "logout",
    "admin_logout",
    "healthz",
    "static",
    "pwa_manifest",
    "pwa_icon",
    "pwa_service_worker",
    "github_dispatch",
})


@app.before_request
def require_game_authentication():
    endpoint = request.endpoint
    if endpoint is None or endpoint in PUBLIC_ENDPOINTS:
        return None

    if is_admin() or get_current_participant(request):
        return None

    if request.method in ("GET", "HEAD"):
        return redirect(url_for("login"))
    return "Connexion requise.", 401


# ------------------ Championnat state ------------------
def championnat_state_row_id():
    return 1 if selected_season_year() == 2026 else selected_season_year()


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
                WHERE id=:sid
            """), {"sid": championnat_state_row_id()}).fetchone()

        if row:
            return {
                "is_open": bool(row[0]),
                "revealed": bool(row[1]),
                "locked_at": str(row[2]) if row[2] else None,
            }
        return default

    path = os.path.join(DATA_DIR, f"championnat_state_{selected_season_year()}.json")
    if selected_season_year() == 2026 and not os.path.exists(path):
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
                VALUES (:sid, :is_open, :revealed, :locked_at, NOW())
                ON CONFLICT (id)
                DO UPDATE SET
                    is_open = EXCLUDED.is_open,
                    revealed = EXCLUDED.revealed,
                    locked_at = EXCLUDED.locked_at,
                    updated_at = NOW()
            """), {**state, "sid": championnat_state_row_id()})
    else:
        path = os.path.join(DATA_DIR, f"championnat_state_{selected_season_year()}.json")
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
                WHERE season_year=:season
                ORDER BY COALESCE(player_norm, LOWER(BTRIM(player_name))), updated_at DESC
            """), {"season": selected_season_year()}).fetchall()

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
    name = participant["pseudo"] if participant else ""
    pid = req.cookies.get("player_id")
    if not pid:
        pid = str(uuid.uuid4())
    return name, pid


# ------------------ Week-end open/close DB state ------------------
def is_weekend_closed(weekend_id: str) -> bool:
    weekend = get_weekend(weekend_id)
    if weekend and is_season_archived(weekend_season(weekend)):
        return True
    deadline = deadline_from_weekend(weekend)
    if deadline is not None and datetime.now(timezone.utc) >= deadline:
        return True
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
    weekend = get_weekend(weekend_id)
    deadline = deadline_from_weekend(weekend)
    if deadline is not None and datetime.now(timezone.utc) >= deadline:
        if bool((weekend or {}).get("auto_publish", False)):
            return True
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
    if pole_pred and pole_real and normalize(pole_pred) == normalize(pole_real):
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
    pole_ok = bool(pole_pred and pole_real and normalize(pole_pred) == normalize(pole_real))
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


def bonus_detail(pred_bonus: dict, real_bonus: dict, weekend_bonus_questions: list, *, player_name=None, reviews=None):
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
        ok, manually_reviewed = evaluated_bonus_answer(
            b, pred, real, player_name=player_name, reviews=reviews)
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
            "manual": manually_reviewed,
            "pts": pts
        })

    return {"items": items, "total": round(total, 2)}


def compute_points_breakdown(prono: dict, results: dict, w: dict, *, player_name=None, reviews=None):
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

    bonus = bonus_detail(prono.get("bonus", {}), results.get("bonus", {}), bq,
                         player_name=player_name, reviews=reviews)

    total = round(qual["total"] + sprint["total"] + gp["total"] + bonus["total"], 2)

    return {
        "qualif": qual,
        "sprint": sprint,
        "gp": gp,
        "bonus": bonus,
        "total": total
    }


def build_gp_analysis(rows: list, results: dict) -> list:
    """Débrief factuel du GP, sans modifier le barème ou les pronostics."""
    if not rows or not results:
        return []

    official_gp = results.get("gp") or []
    official_sprint = results.get("sprint") or []
    pole = results.get("pole")
    has_gp = len(official_gp) >= 3 and all(official_gp[:3])
    has_sprint = len(official_sprint) >= 3 and all(official_sprint[:3])
    if not (has_gp or has_sprint or pole):
        return []

    count = len(rows)
    comments = []
    top_score = rows[0]["pts"]["total"]
    leaders = [r["player"] for r in rows if r["pts"]["total"] == top_score]
    if len(leaders) == 1:
        gap = round(top_score - rows[1]["pts"]["total"], 2) if count > 1 else None
        detail = f" avec {top_score:g} points"
        if gap is not None:
            detail += f", soit {gap:g} point(s) d'avance sur {rows[1]['player']}"
        comments.append({"icon": "🏆", "title": "Performance du GP",
                         "text": f"{leaders[0]} mène le classement calculé{detail}."})
    else:
        comments.append({"icon": "🏆", "title": "Duel au sommet",
                         "text": f"{', '.join(leaders)} sont à égalité en tête avec {top_score:g} points."})

    if has_gp:
        winner = official_gp[0]
        predicted = sum(normalize(r["p"].get("gp_p1")) == normalize(winner)
                        for r in rows)
        if predicted == 0:
            outcome = f"Victoire surprise de {winner} : aucun des {count} joueurs ne l'avait placé P1."
        else:
            outcome = f"{predicted} joueur(s) sur {count} avaient annoncé la victoire de {winner}."
        comments.append({"icon": "🏁", "title": "Lecture de la course", "text": outcome})

        exact = sum(all(normalize(r["p"].get(f"gp_p{i+1}")) == normalize(official_gp[i])
                        for i in range(3)) for r in rows)
        if exact:
            names = [r["player"] for r in rows
                     if all(normalize(r["p"].get(f"gp_p{i+1}")) == normalize(official_gp[i])
                            for i in range(3))]
            outcome = f"Podium GP parfait pour {', '.join(names)} : les trois positions exactes !"
        else:
            outcome = "Le podium GP dans l'ordre exact a échappé à tous les joueurs."
        comments.append({"icon": "🎯", "title": "Podium du dimanche", "text": outcome})

    if has_sprint:
        exact = [r["player"] for r in rows
                 if all(normalize(r["p"].get(f"sprint_p{i+1}")) == normalize(official_sprint[i])
                        for i in range(3))]
        if exact:
            outcome = f"{', '.join(exact)} ont trouvé le podium Sprint parfait."
        else:
            winner = official_sprint[0]
            winner_picks = sum(normalize(r["p"].get("sprint_p1")) == normalize(winner)
                               for r in rows)
            outcome = (f"Personne n'a trouvé le podium Sprint exact ; "
                       f"{winner_picks} joueur(s) avaient toutefois annoncé {winner} vainqueur.")
        comments.append({"icon": "⚡", "title": "Débrief du Sprint", "text": outcome})

    if pole:
        correct = [r["player"] for r in rows
                   if normalize(r["p"].get("pole")) == normalize(pole)]
        if correct:
            outcome = f"La pole de {pole} a été trouvée par {len(correct)} joueur(s) sur {count}."
        else:
            outcome = f"La pole de {pole} a déjoué tous les pronostics."
        comments.append({"icon": "⏱️", "title": "Qualifications", "text": outcome})

    bonus_answers = results.get("bonus") or {}
    if any(value is not None and str(value).strip() for value in bonus_answers.values()):
        success = sum(sum(bool(item["ok"]) for item in r["pts"]["bonus"]["items"])
                      for r in rows)
        outcome = (f"{success} bonne(s) réponse(s) aux questions bonus "
                   f"pour l'ensemble des {count} joueurs.")
        comments.append({"icon": "✨", "title": "Questions bonus", "text": outcome})

    return comments[:6]


# ------------------ Fichiers fallback ------------------
def pronos_path(weekend_id):
    return os.path.join(PRONOS_DIR, f"{weekend_id}.json")


def results_path(weekend_id):
    return os.path.join(RESULTS_DIR, f"{weekend_id}.json")


def championnat_path():
    year = selected_season_year()
    return os.path.join(PRONOS_DIR, "championnat.json" if year == 2026 else f"championnat_{year}.json")


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


# ------------------ Historique pronos ------------------
def save_prono_history(conn, weekend_id: str, user_key: str, player_name: str, player_norm: str,
                       participant_id, payload: dict, user_agent: str = "", source: str = "save"):
    conn.execute(text("""
        INSERT INTO pronos_history (
            weekend_id, user_key, player_name, player_norm, participant_id,
            payload_json, source, user_agent, saved_at
        )
        VALUES (
            :w, :u, :n, :pn, :pidb,
            CAST(:p AS jsonb), :src, :ua, NOW()
        )
    """), {
        "w": weekend_id,
        "u": user_key,
        "n": player_name,
        "pn": player_norm,
        "pidb": participant_id,
        "p": json.dumps(payload, ensure_ascii=False),
        "src": source or "save",
        "ua": (user_agent or "")[:1000]
    })


def get_prono_history_for_weekend(weekend_id: str, limit: int = 200):
    if not engine:
        return []

    ensure_db_bootstrap()
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT id, weekend_id, player_name, user_key, participant_id,
                   payload_json, source, user_agent, saved_at
            FROM pronos_history
            WHERE weekend_id=:w
            ORDER BY saved_at DESC, id DESC
            LIMIT :lim
        """), {"w": weekend_id, "lim": int(limit)}).fetchall()

    out = []
    for r in rows:
        payload = dict(r[5] or {})
        out.append({
            "id": r[0],
            "weekend_id": r[1],
            "player_name": r[2],
            "user_key": r[3],
            "participant_id": r[4],
            "payload": payload,
            "source": r[6],
            "user_agent": r[7] or "",
            "saved_at": str(r[8]) if r[8] else None,
            "saved_at_fr": format_dt_fr(r[8]),
        })
    return out


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


# ================== ACCUEIL / RACE CENTER ==================
def home_upcoming_weekends(weekends: list, *, today=None, exclude_id=None, limit=3) -> list:
    """Ne montre jamais les GP passés : accessibles via « Résultats par GP »."""
    today = today or date.today()
    coming = [
        w for w in weekends
        if not w.get("cancelled")
        and w.get("date_obj") is not None
        and w["date_obj"] >= today
        and w.get("id") != exclude_id
    ]
    coming.sort(key=lambda w: (w["date_obj"], (w.get("label") or "").lower()))
    return coming[:max(0, limit)]


@app.route("/")
def home():
    name, _ = current_player(request)
    all_weekends = get_sorted_weekends()
    season_year = selected_season_year()
    years = sorted(set([get_season_year()] + [weekend_season(w) for w in all_weekends]))
    season_weekends = [w for w in all_weekends if weekend_season(w) == season_year]

    # Le Race Center est la priorité ; les prochaines épreuves viennent APRÈS,
    # sans répéter le GP déjà mis en avant.
    active_gp = race_center(season_weekends)

    def player_has_prono(weekend_id):
        if not name:
            return False
        saved = get_latest_pronos_by_player_for_weekend(weekend_id)
        return normalize_pseudo(name) in {
            normalize_pseudo(pseudo) for pseudo in saved
        }

    if active_gp:
        active_gp["closed"] = is_weekend_closed(active_gp["id"])
        active_gp["public"] = is_pronos_public(active_gp["id"])
        active_gp["played"] = player_has_prono(active_gp["id"])

    upcoming = home_upcoming_weekends(
        season_weekends,
        exclude_id=active_gp["id"] if active_gp else None,
        limit=3
    )
    upcoming_cards = []
    for weekend in upcoming:
        item = dict(weekend)
        item["closed"] = is_weekend_closed(item["id"])
        item["played"] = player_has_prono(item["id"])
        upcoming_cards.append(item)

    return render_template(
        "index.html",
        name=name,
        upcoming=upcoming_cards,
        race_center=active_gp,
        season_year=season_year,
        years=years
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
        session["authenticated_player"] = participant["pseudo_normalized"]
        session["authenticated_player_id"] = participant["id"]
        session["authenticated_secret_marker"] = participant_secret_marker(participant)
        session.permanent = True
        resp = make_response(redirect(url_for("profile")))
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
    session.pop("authenticated_player", None)
    session.pop("authenticated_player_id", None)
    session.pop("authenticated_secret_marker", None)
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
                  AND season_year=:season
                ORDER BY updated_at DESC
                LIMIT 1
            """), {"pn": player_norm, "season": selected_season_year()}).fetchone()
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
        if is_season_archived(selected_season_year()):
            flash("Cette saison est archivée.")
            return redirect(url_for("championnat", season=selected_season_year()))
        if not state.get("is_open", True):
            flash("Les pronostics championnat du monde sont fermés.")
            return redirect(url_for("championnat", season=selected_season_year()))

        form = request.form
        picks = [form.get("wc_p1"), form.get("wc_p2"), form.get("wc_p3")]
        v = [x for x in picks if x]
        if len(set(v)) != len(v):
            flash("Doublon détecté : tu ne peux pas mettre le même pilote 2 fois.")
            return redirect(url_for("championnat", season=selected_season_year()))

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
                      AND season_year=:season
                    ORDER BY updated_at DESC
                    LIMIT 1
                """), {"pn": player_norm, "season": selected_season_year()}).fetchone()

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
                            user_key, player_name, player_norm, participant_id, season_year,
                            payload_json, created_at, updated_at
                        )
                        VALUES (
                            :u, :n, :pn, :pidb, :season,
                            CAST(:p AS jsonb), NOW(), NOW()
                        )
                    """), {
                        "u": pid,
                        "n": name,
                        "pn": player_norm,
                        "pidb": participant["id"],
                        "p": json.dumps(payload, ensure_ascii=False),
                        "season": selected_season_year()
                    })
        else:
            all_preds = load_json(championnat_path(), {})
            payload["updated_at"] = datetime.utcnow().isoformat()
            payload["player_name"] = name
            all_preds[player_norm] = payload
            save_json(championnat_path(), all_preds)

        flash("Pronostic championnat enregistré ✅")
        resp = make_response(redirect(url_for("championnat", season=selected_season_year())))
        resp.set_cookie("player_name", name, max_age=60 * 60 * 24 * 365)
        resp.set_cookie("player_id", pid, max_age=60 * 60 * 24 * 365)
        return resp

    return render_template(
        "championnat.html",
        riders=get_riders_for_season(selected_season_year()),
        my=my,
        name=name,
        championnat_state=state,
        season_year=selected_season_year(),
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

    closed = is_weekend_closed(weekend_id)

    if closed and is_pronos_public(weekend_id):
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
            my["_last_saved_at"] = str(row[2]) if row[2] else None
            my["_last_saved_at_fr"] = format_dt_fr(row[2]) if row[2] else ""
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
        if is_season_archived(weekend_season(w)):
            flash("Cette saison est archivée : pronostics non modifiables.")
            return redirect(url_for("pronos", weekend_id=weekend_id))
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
            ua = request.headers.get("User-Agent", "")
            try:
                with engine.begin() as conn:
                    save_prono_history(
                        conn=conn,
                        weekend_id=weekend_id,
                        user_key=pid,
                        player_name=name,
                        player_norm=player_norm,
                        participant_id=participant["id"],
                        payload=payload,
                        user_agent=ua,
                        source="save"
                    )

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

                app.logger.info(
                    "PRONO_SAVE weekend_id=%s player_name=%s participant_id=%s user_key=%s",
                    weekend_id, name, participant["id"], pid
                )
            except Exception:
                app.logger.exception(
                    "PRONO_SAVE_ERROR weekend_id=%s player_name=%s participant_id=%s user_key=%s",
                    weekend_id, name, participant["id"], pid
                )
                flash("Erreur lors de l'enregistrement du pronostic.")
                return redirect(url_for("pronos", weekend_id=weekend_id))
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
        riders=get_riders_for_season(weekend_season(w)),
        all_teams=get_teams_for_season(weekend_season(w)),
        official_teams=get_teams_for_season(weekend_season(w), kind="official"),
        satellite_teams=get_teams_for_season(weekend_season(w), kind="satellite"),
        bonus_type_choices=BONUS_TYPE_CHOICES,
        my=my,
        name=name,
        closed=closed,
        is_public=is_pronos_public(weekend_id),
        admin_enabled=admin_enabled(),
        is_admin=is_admin()
    )


# ================== ADMIN AUTH ==================
@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if not admin_enabled():
        return "Admin non configuré (ADMIN_USER/ADMIN_PASS manquants).", 500

    if is_admin():
        return redirect(url_for("admin_home"))

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
            "public": is_pronos_public(wid) if engine else False,
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


def check_participant_admin_csrf():
    token = session.get("v2_csrf")
    if not token or not hmac.compare_digest(
        str(request.form.get("csrf") or ""), str(token)
    ):
        abort(403)


@app.route("/admin/participants", methods=["GET", "POST"])
@require_admin
def admin_participants():
    if request.method == "POST":
        check_participant_admin_csrf()
        pseudo = (request.form.get("pseudo") or "").strip()
        secret = (request.form.get("secret") or "").strip()
        ok, msg = add_participant(pseudo, secret)
        flash(msg)
        return redirect(url_for("admin_participants"))

    participants = get_all_participants(include_inactive=True)
    if not session.get("v2_csrf"):
        session["v2_csrf"] = secrets.token_urlsafe(32)
    name, _ = current_player(request)
    reset_notice = session.pop("admin_password_reset_notice", None)
    return render_template(
        "admin_participants.html",
        name=name,
        participants=participants, csrf=session["v2_csrf"],
        can_manage_admin_roles=is_root_admin(),
        reset_notice=reset_notice
    )


@app.route("/admin/participants/<int:participant_id>/admin_role", methods=["POST"])
@require_admin
def admin_participant_role(participant_id):
    # Seul le compte administrateur principal accorde les délégations.
    if not is_root_admin():
        abort(403)
    check_participant_admin_csrf()
    requested = request.form.get("grant_admin")
    if requested not in (None, "1"):
        abort(400)
    ok, message = set_participant_admin_role(participant_id, requested == "1")
    flash(message)
    return redirect(url_for("admin_participants"))


@app.route("/admin/participants/<int:participant_id>/toggle", methods=["POST"])
@require_admin
def admin_toggle_participant(participant_id):
    check_participant_admin_csrf()
    ok, msg = toggle_participant(participant_id)
    flash(msg)
    return redirect(url_for("admin_participants"))


@app.route("/admin/participants/<int:participant_id>/reset_secret", methods=["POST"])
@require_admin
def admin_reset_participant_secret(participant_id):
    check_participant_admin_csrf()
    new_secret = request.form.get("new_secret") or ""
    confirmation = request.form.get("new_secret_confirm") or ""
    if new_secret != confirmation:
        ok, msg = False, "Les deux nouveaux mots de passe ne correspondent pas."
    else:
        try:
            ok, msg = reset_participant_secret(participant_id, new_secret)
        except Exception:
            # Ne jamais journaliser les mots de passe fournis.
            app.logger.exception("ADMIN_PASSWORD_RESET_ERROR participant_id=%s", participant_id)
            ok, msg = False, "Réinitialisation impossible. Réessaie ou consulte les journaux du serveur."
    # Afficher le résultat juste à côté du joueur concerné : sur téléphone,
    # un simple flash tout en haut de la longue liste passe inaperçu.
    session["admin_password_reset_notice"] = {
        "participant_id": participant_id,
        "ok": bool(ok),
        "message": msg
    }
    flash(msg)
    return redirect(url_for("admin_participants") + f"#participant-{participant_id}")


@app.route("/admin/participants/<int:participant_id>/delete", methods=["POST"])
@require_admin
def admin_delete_participant(participant_id):
    check_participant_admin_csrf()
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
        season_year=selected_season_year(),
        count=count,
        pronos=pronos_list
    )


@app.route("/admin/championnat/open", methods=["POST"])
@require_admin
def admin_open_championnat():
    open_championnat()
    flash("🟢 Pronostics championnat OUVERTS")
    return redirect(url_for("admin_championnat", season=selected_season_year()))


@app.route("/admin/championnat/close", methods=["POST"])
@require_admin
def admin_close_championnat():
    close_championnat()
    flash("🔒 Pronostics championnat FERMÉS")
    return redirect(url_for("admin_championnat", season=selected_season_year()))


@app.route("/admin/championnat/reveal", methods=["POST"])
@require_admin
def admin_reveal_championnat():
    state = get_championnat_state()
    if state.get("is_open", True):
        flash("Ferme d’abord les pronostics avant de les divulguer.")
        return redirect(url_for("admin_championnat", season=selected_season_year()))

    reveal_championnat()
    flash("👁️ Pronostics championnat divulgués")
    return redirect(url_for("admin_championnat", season=selected_season_year()))


@app.route("/admin/championnat/hide", methods=["POST"])
@require_admin
def admin_hide_championnat():
    hide_championnat()
    flash("🙈 Pronostics championnat masqués")
    return redirect(url_for("admin_championnat", season=selected_season_year()))


@app.route("/admin/w/<weekend_id>")
@require_admin
def admin_weekend(weekend_id):
    w = get_weekend(weekend_id)
    if not w:
        return "Week-end inconnu", 404

    count = count_distinct_pronos_for_weekend(weekend_id)
    closed = is_weekend_closed(weekend_id) if engine else False
    public = is_pronos_public(weekend_id) if engine else False

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


@app.route("/admin/w/<weekend_id>/history")
@require_admin
def admin_weekend_history(weekend_id):
    w = get_weekend(weekend_id)
    if not w:
        return "Week-end inconnu", 404
    if not engine:
        return "DB non configurée (DATABASE_URL manquant).", 500

    rows = get_prono_history_for_weekend(weekend_id, limit=300)

    html = """
    <!doctype html>
    <html lang="fr">
    <head>
      <meta charset="utf-8">
      <title>Historique pronos — {{ w.label }}</title>
      <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #0f172a; color: #e5e7eb; }
        a { color: #93c5fd; }
        table { width: 100%; border-collapse: collapse; background: #111827; }
        th, td { border: 1px solid #374151; padding: 8px; vertical-align: top; text-align: left; }
        th { background: #1f2937; }
        code, pre { white-space: pre-wrap; word-break: break-word; }
        .wrap { max-width: 1400px; margin: 0 auto; }
        .top { margin-bottom: 16px; }
        .small { font-size: 12px; opacity: 0.85; }
      </style>
    </head>
    <body>
      <div class="wrap">
        <div class="top">
          <h1>Historique des sauvegardes — {{ w.label }}</h1>
          <p><a href="{{ url_for('admin_weekend', weekend_id=w.id) }}">← Retour au week-end admin</a></p>
          <p class="small">Dernières sauvegardes historisées. Chaque ligne = une validation de formulaire.</p>
        </div>

        {% if rows %}
        <table>
          <thead>
            <tr>
              <th>Heure</th>
              <th>Joueur</th>
              <th>Source</th>
              <th>User key</th>
              <th>User-Agent</th>
              <th>Prono</th>
            </tr>
          </thead>
          <tbody>
          {% for r in rows %}
            <tr>
              <td>{{ r.saved_at_fr or r.saved_at }}</td>
              <td>{{ r.player_name }}</td>
              <td>{{ r.source }}</td>
              <td><code>{{ r.user_key }}</code></td>
              <td class="small">{{ r.user_agent }}</td>
              <td><pre>{{ r.payload | tojson(indent=2) }}</pre></td>
            </tr>
          {% endfor %}
          </tbody>
        </table>
        {% else %}
          <p>Aucun historique trouvé pour ce GP.</p>
        {% endif %}
      </div>
    </body>
    </html>
    """
    return render_template_string(html, w=w, rows=rows)


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
            "public": is_pronos_public(wid) if engine else False,
            "cancelled": cancelled,
            "status": status,
            "count": count_distinct_pronos_for_weekend(wid),
            "can_edit_date": not is_season_archived(weekend_season(w)),
            "can_cancel": not closed and not cancelled,
            "can_reactivate": not closed and cancelled,
        })

    if not session.get("v2_csrf"):
        session["v2_csrf"] = secrets.token_urlsafe(32)
    name, _ = current_player(request)
    return render_template(
        "admin_calendar.html",
        name=name,
        csrf=session["v2_csrf"],
        weekends=items
    )


@app.route("/admin/calendar/<weekend_id>/update", methods=["POST"])
@require_admin
def admin_calendar_update(weekend_id):
    w = get_weekend(weekend_id)
    if not w:
        return "Week-end inconnu", 404

    if is_season_archived(weekend_season(w)):
        flash("Saison archivée : calendrier non modifiable.")
        return redirect(url_for("admin_calendar"))
    if not hmac.compare_digest(str(request.form.get("csrf") or ""),
                               str(session.get("v2_csrf") or "")) or not session.get("v2_csrf"):
        return "Formulaire de modification invalide.", 403

    new_date = (request.form.get("date") or "").strip()
    new_label = (request.form.get("label") or "").strip()

    if new_date:
        new_day = parse_weekend_date(new_date, weekend_season(w))
        if not new_day or new_day.year != weekend_season(w):
            flash("La date du GP doit rester dans sa saison.")
            return redirect(url_for("admin_calendar"))
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
            official_question = dict(b)
            if official_question.get("type") == "rider":
                official_question["resolved_options"] = get_riders_for_season(
                    weekend_season(w), active_only=False)
            elif official_question.get("type") in {"team", "official_team", "satellite_team"}:
                kind = {"official_team":"official", "satellite_team":"satellite"}.get(
                    official_question.get("type"))
                official_question["resolved_options"] = get_teams_for_season(
                    weekend_season(w), kind=kind, active_only=False)
            ok, err = validate_bonus_answer_for_question(official_question, answer)
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
        riders=get_riders_for_season(weekend_season(w), active_only=False),
        all_teams=get_teams_for_season(weekend_season(w), active_only=False),
        official_teams=get_teams_for_season(weekend_season(w), kind="official", active_only=False),
        satellite_teams=get_teams_for_season(weekend_season(w), kind="satellite", active_only=False),
        bonus_type_choices=BONUS_TYPE_CHOICES,
        results=results
    )


@app.route("/admin/w/<weekend_id>/bonus-reviews", methods=["GET", "POST"])
@require_admin
def admin_bonus_reviews(weekend_id):
    """Validation ponctuelle, réversible et horodatée sans toucher au prono brut."""
    w = get_weekend(weekend_id)
    if not w:
        return "Week-end inconnu", 404
    if not engine:
        return "Base PostgreSQL requise pour la validation manuelle.", 503
    results = load_results(weekend_id) or {}
    questions = w.get("bonus_questions", []) or []
    pronos_by_player = get_latest_pronos_by_player_for_weekend(weekend_id)
    token = session.get("bonus_review_csrf")
    if not token:
        token = secrets.token_urlsafe(32)
        session["bonus_review_csrf"] = token

    if request.method == "POST":
        if not hmac.compare_digest(
            str(request.form.get("csrf_token") or ""), str(token)):
            return "Formulaire expiré ou non autorisé", 403
        pname = str(request.form.get("player") or "")
        qid = str(request.form.get("question") or "")
        decision = str(request.form.get("decision") or "")
        question = next((b for b in questions if b.get("id") == qid), None)
        player_name = next(
            (p for p in pronos_by_player if normalize_pseudo(p) == normalize_pseudo(pname)),
            None)
        if not question or player_name is None or decision not in ("auto", "valid", "invalid"):
            return "Correction invalide", 400
        predicted = (pronos_by_player[player_name].get("bonus") or {}).get(qid)
        official = (results.get("bonus") or {}).get(qid)
        if decision != "auto" and not official:
            flash("Saisir et enregistrer d'abord la bonne réponse officielle.")
            return redirect(url_for("admin_bonus_reviews", weekend_id=weekend_id))
        key = {"wid": weekend_id, "player": normalize_pseudo(player_name), "qid": qid}
        with engine.begin() as conn:
            if decision == "auto":
                conn.execute(text("""
                    DELETE FROM bonus_reviews
                    WHERE weekend_id=:wid AND player_norm=:player AND question_id=:qid
                """), key)
            else:
                conn.execute(text("""
                    INSERT INTO bonus_reviews (
                        weekend_id, player_norm, question_id, decision,
                        prediction_snapshot, official_snapshot, reviewed_by
                    ) VALUES (
                        :wid, :player, :qid, :decision, :prediction, :official, :admin
                    )
                    ON CONFLICT (weekend_id, player_norm, question_id)
                    DO UPDATE SET decision=EXCLUDED.decision,
                        prediction_snapshot=EXCLUDED.prediction_snapshot,
                        official_snapshot=EXCLUDED.official_snapshot,
                        reviewed_by=EXCLUDED.reviewed_by, reviewed_at=NOW()
                """), {**key, "decision": decision == "valid",
                       "prediction": str(predicted or ""),
                       "official": str(official or ""), "admin": ADMIN_USER})
        flash("Validation mise à jour ; les classements sont recalculés automatiquement.")
        return redirect(url_for("admin_bonus_reviews", weekend_id=weekend_id))

    reviews = load_bonus_reviews(weekend_id)
    entries = []
    for b in questions:
        real = (results.get("bonus") or {}).get(b.get("id"))
        answers = []
        for player_name, prediction in sorted(pronos_by_player.items()):
            pred = (prediction.get("bonus") or {}).get(b.get("id"))
            valid, manual = evaluated_bonus_answer(
                b, pred, real, player_name=player_name, reviews=reviews)
            review = reviews.get((normalize_pseudo(player_name), b.get("id")))
            stale = review is not None and not manual
            answers.append({"player": player_name, "pred": pred, "valid": valid,
                            "automatic": is_bonus_answer_correct(b, pred, real),
                            "manual": manual, "stale": stale,
                            "decision": ("valid" if valid else "invalid") if manual else "auto"})
        entries.append({"question": b, "official": real, "answers": answers})
    return render_template("admin_bonus_reviews.html",
        w=w, entries=entries, csrf_token=token,
        name=current_player(request)[0])


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

    # Ne dévoiler aucune comparaison avant la clôture ET la publication.
    if not is_weekend_closed(weekend_id) or not is_pronos_public(weekend_id):
        return "Les pronostics de ce GP ne sont pas encore rendus publics.", 403

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
        insights=build_public_insights(pronos_list, w),
        closed=True,
        public=True,
        admin_enabled=admin_enabled(),
        is_admin=is_admin()
    )


@app.route("/results_by_race")
def results_by_race():
    name, _ = current_player(request)

    gp_id = (request.args.get("gp") or "").strip()
    selected = get_weekend(gp_id) if gp_id else None

    # Un lien direct vers un GP archivé doit afficher le sélecteur de sa saison.
    season_year = selected_season_year()
    if selected and not request.args.get("season"):
        season_year = weekend_season(selected)
    weekends = [w for w in get_sorted_weekends() if weekend_season(w) == season_year]

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

    reviews = load_bonus_reviews(gp_id)
    rows = []
    for item in pronos_list:
        p = item["payload"] or {}
        breakdown = compute_points_breakdown(
            p, results, selected, player_name=item["player_name"], reviews=reviews)
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
        analysis=build_gp_analysis(rows, results),
        notice=None,
        admin_enabled=admin_enabled(),
        is_admin=is_admin()
    )


@app.route("/w/<weekend_id>/classement")
def classement_weekend(weekend_id):
    """Compatibilité des anciens liens : une seule page résultats et classement."""
    if not get_weekend(weekend_id):
        return "Week-end inconnu", 404
    return redirect(url_for("results_by_race", gp=weekend_id))


@app.route("/classement")
def classement_general():
    name, _ = current_player(request)

    weekends = [w for w in get_sorted_weekends() if weekend_season(w) == selected_season_year()]
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
    gp_rounds = []

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
        reviews = load_bonus_reviews(wid)
        round_scores = {}

        for player_name, p in pronos_by_player.items():
            pts = compute_points_breakdown(
                p, results, w, player_name=player_name, reviews=reviews)
            q_score = pts["qualif"]["total"]
            s_score = pts["sprint"]["total"]
            g_score = pts["gp"]["total"]
            bonus_score = pts["bonus"]["total"]
            total_gp = pts["total"]
            round_scores[player_name] = total_gp

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

        gp_rounds.append({"id": wid, "label": w.get("label", wid),
                          "scores": round_scores})

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
        weekends=gp_scored,
        season_year=selected_season_year(),
        championship_analysis=build_championship_insights(rows, gp_rounds),
        notice=notice,
        admin_enabled=admin_enabled(),
        is_admin=is_admin()
    )


# ------------------ Health checks ------------------
@app.route("/healthz")
def healthz():
    return "OK", 200


@app.route("/health")
def health():
    if engine:
        try:
            ensure_db_bootstrap()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except Exception as e:
            return f"DB ERROR: {e}", 500
    return "OK", 200

register_v2(app, {
    "engine": engine,
    "get_current_participant": get_current_participant,
    "get_participant_by_input": get_participant_by_input,
    "change_participant_secret": change_participant_secret,
    "participant_secret_marker": participant_secret_marker,
    "get_all_participants": get_all_participants,
    "current_player": current_player,
    "get_weekend": get_weekend,
    "get_sorted_weekends": get_sorted_weekends,
    "load_weekends_data": load_weekends_data,
    "save_weekends_data": save_weekends_data,
    "update_weekend_config_fields": update_weekend_config_fields,
    "update_weekend_configs_bulk": update_weekend_configs_bulk,
    "weekend_config_revision": weekend_config_revision,
    "ensure_db_bootstrap": ensure_db_bootstrap,
    "is_admin": is_admin,
    "normalize_pseudo": normalize_pseudo,
    "get_latest_pronos_by_player_for_weekend": get_latest_pronos_by_player_for_weekend,
    "load_results": load_results,
    "load_bonus_reviews": load_bonus_reviews,
    "compute_points_breakdown": compute_points_breakdown,
    "is_weekend_closed": is_weekend_closed,
    "is_pronos_public": is_pronos_public,
    "is_season_archived": is_season_archived,
    "weekend_season": weekend_season,
    "selected_season_year": selected_season_year,
    "get_season_year": get_season_year,
    "get_riders_for_season": get_riders_for_season,
    "get_teams_for_season": get_teams_for_season,
})

register_bulk_import(app, {
    "engine": engine,
    "is_admin": is_admin,
    "current_player": current_player,
    "selected_season_year": selected_season_year,
    "weekend_season": weekend_season,
    "get_sorted_weekends": get_sorted_weekends,
    "is_season_archived": is_season_archived,
    "weekend_config_revision": weekend_config_revision,
    "update_weekend_configs_bulk": update_weekend_configs_bulk,
})

register_notifications(app, {
    "engine": engine,
    "get_current_participant": get_current_participant,
    "get_all_participants": get_all_participants,
    "get_sorted_weekends": get_sorted_weekends,
    "get_weekend": get_weekend,
    "current_player": current_player,
    "is_admin": is_admin,
    "is_season_archived": is_season_archived,
    "weekend_season": weekend_season,
    "normalize_pseudo": normalize_pseudo,
    "deadline_from_weekend": deadline_from_weekend,
    "is_weekend_closed": is_weekend_closed,
})

print("APP FLASK DEMARREE", flush=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
