# app.py — PostgreSQL persistant
# + ADMIN login (nom + mdp) + page publique HTML
# + Page "Résultats par course" (dropdown GP + détail points)
# + Switch admin ON/OFF pour ouvrir/fermer les pronos (fermé = public)
# + Auto-redirect : si GP fermé, /w/<id>/pronos => /w/<id>/public/pronos
# + ✅ MAJ anti-doublons : 1 seule ligne par pseudo (dernier prono) dans :
#     - Classement (/w/<id>/classement)
#     - Pronos publics (/w/<id>/public/pronos)
#     - Résultats par course (/results_by_race)
#   (car un même joueur peut avoir plusieurs user_key si cookies perdus)
# + ✅ MAJ IMPORTANT : Résultats officiels persistants en DB (sinon Render "perd" les fichiers)
# + ✅ NOUVEAU : Classement général saison (/classement)

import os, json, uuid
from datetime import datetime, date, timedelta
from functools import wraps

from flask import (
    Flask, render_template, request, redirect, url_for,
    make_response, flash, session
)
from sqlalchemy import create_engine, text

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")
app.permanent_session_lifetime = timedelta(days=365)

# ✅ Admin login / password (Render > Environment)
ADMIN_USER = os.environ.get("ADMIN_USER", "")
ADMIN_PASS = os.environ.get("ADMIN_PASS", "")

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


DATA_DIR = "data"
PRONOS_DIR = os.path.join(DATA_DIR, "pronos")
RESULTS_DIR = os.path.join(DATA_DIR, "results")
os.makedirs(PRONOS_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)

WEEKENDS_FILE = os.path.join(DATA_DIR, "weekends.json")

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

# ------------------ DB (PostgreSQL) ------------------
DATABASE_URL = os.environ.get("DATABASE_URL")
engine = None
if DATABASE_URL:
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

def db_init():
    if not engine:
        return
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS weekends (
            weekend_id TEXT PRIMARY KEY,
            closed_at TIMESTAMPTZ NULL
        );
        """))
        # ✅ Ajout colonnes (idempotent)
        conn.execute(text("ALTER TABLE weekends ADD COLUMN IF NOT EXISTS pronos_public_at TIMESTAMPTZ NULL;"))
        conn.execute(text("ALTER TABLE weekends ADD COLUMN IF NOT EXISTS results_published_at TIMESTAMPTZ NULL;"))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS pronos (
            id SERIAL PRIMARY KEY,
            weekend_id TEXT NOT NULL,
            user_key TEXT NOT NULL,
            player_name TEXT NOT NULL,
            payload_json JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (weekend_id, user_key)
        );
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_pronos_weekend ON pronos(weekend_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_pronos_weekend_player ON pronos(weekend_id, player_name);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_pronos_updated_at ON pronos(updated_at);"))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS championnat_pronos (
            id SERIAL PRIMARY KEY,
            user_key TEXT NOT NULL UNIQUE,
            player_name TEXT NOT NULL,
            payload_json JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """))

        # ✅ NOUVEAU : résultats persistants (Render ne garde pas les fichiers)
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS results (
            weekend_id TEXT PRIMARY KEY,
            payload_json JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """))

db_init()

def is_weekend_closed(weekend_id: str) -> bool:
    if not engine:
        return False
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT closed_at FROM weekends WHERE weekend_id=:w"),
            {"w": weekend_id}
        ).fetchone()
    return bool(row and row[0])

def is_pronos_public(weekend_id: str) -> bool:
    # (conservé pour compat / historiques) - chez toi on utilise "fermé = public"
    if not engine:
        return False
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT pronos_public_at FROM weekends WHERE weekend_id=:w"),
            {"w": weekend_id}
        ).fetchone()
    return bool(row and row[0])

def close_and_publish_pronos(weekend_id: str):
    """Compat: ancienne action. Règle : dès la clôture, les pronos deviennent publics."""
    if not engine:
        return
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO weekends (weekend_id, closed_at, pronos_public_at)
            VALUES (:w, NOW(), NOW())
            ON CONFLICT (weekend_id)
            DO UPDATE SET
              closed_at = NOW(),
              pronos_public_at = NOW()
        """), {"w": weekend_id})

# ✅ SWITCH ON/OFF
def set_weekend_open(weekend_id: str):
    """ON = ouvert : on remet closed_at/pronos_public_at à NULL (donc privé)."""
    if not engine:
        return
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO weekends (weekend_id, closed_at, pronos_public_at)
            VALUES (:w, NULL, NULL)
            ON CONFLICT (weekend_id)
            DO UPDATE SET
              closed_at = NULL,
              pronos_public_at = NULL
        """), {"w": weekend_id})

def set_weekend_closed_and_public(weekend_id: str):
    """OFF = fermé : pronos fermés et rendus publics."""
    if not engine:
        return
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO weekends (weekend_id, closed_at, pronos_public_at)
            VALUES (:w, NOW(), NOW())
            ON CONFLICT (weekend_id)
            DO UPDATE SET
              closed_at = NOW(),
              pronos_public_at = NOW()
        """), {"w": weekend_id})


# ------------------ JSON helpers (fallback) ------------------
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

# ------------------ Weekends helpers ------------------
def load_weekends_data():
    data = load_json(WEEKENDS_FILE, {"weekends": []})
    if isinstance(data, list):
        return {"weekends": data}
    if isinstance(data, dict) and "weekends" in data:
        return data
    return {"weekends": []}

def load_weekends_list():
    return load_weekends_data().get("weekends", [])

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
                "bonus_questions": [
                    {"id": "b1", "label": "Un pilote Ducati sur le podium du GP ?", "type": "bool"},
                    {"id": "b2", "label": "Chute lors du sprint ?", "type": "bool"},
                ],
            }
        ],
    }
    os.makedirs(DATA_DIR, exist_ok=True)
    save_json(WEEKENDS_FILE, demo)

bootstrap_weekends()

def get_weekend(weekend_id):
    for w in load_weekends_list():
        if w.get("id") == weekend_id:
            w.setdefault("bonus_questions", [])
            return w
    return None

# ------------------ Statuts GP ------------------
def get_season_year():
    data = load_weekends_data()
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

def weekend_status(weekend_date: date, open_days_before: int = 10):
    if not weekend_date:
        return "closed"
    today = date.today()
    if weekend_date < today:
        return "past"
    open_from = weekend_date - timedelta(days=open_days_before)
    if today >= open_from:
        return "open"
    return "closed"

# ------------------ Identification joueurs (cookies) ------------------
def current_player(req):
    name = req.cookies.get("player_name")
    pid = req.cookies.get("player_id")
    if not pid:
        pid = str(uuid.uuid4())
    return name, pid

# ------------------ Points ------------------
def normalize(x):
    return (x or "").strip().lower()

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

# ---- Détail complet pour "Résultats par course" ----
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
        pred = (pred_bonus.get(bid) or "").strip().lower()
        real = (real_bonus.get(bid) or "").strip().lower()
        ok = bool(pred and real and pred == real)
        pts = 0.5 if ok else 0.0
        total += pts
        items.append({
            "id": bid,
            "label": b.get("label", bid),
            "pred": pred_bonus.get(bid),
            "real": real_bonus.get(bid),
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

# ------------------ Fichiers (fallback) ------------------
def pronos_path(weekend_id):
    return os.path.join(PRONOS_DIR, f"{weekend_id}.json")

def results_path(weekend_id):
    return os.path.join(RESULTS_DIR, f"{weekend_id}.json")

def championnat_path():
    return os.path.join(PRONOS_DIR, "championnat.json")

# ✅ NOUVEAU : résultats persistants (DB d’abord, fichier en fallback)
def load_results(weekend_id: str):
    """DB prioritaire (persistant Render), sinon fichier JSON (dev/local)."""
    if engine:
        with engine.begin() as conn:
            row = conn.execute(text("""
                SELECT payload_json
                FROM results
                WHERE weekend_id=:w
            """), {"w": weekend_id}).fetchone()
        if row and row[0]:
            return dict(row[0] or {})
    return load_json(results_path(weekend_id), None)

def save_results(weekend_id: str, results: dict):
    """Sauvegarde en DB (si dispo) + fallback fichier."""
    results = results or {}
    if engine:
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

# ------------------ Helpers anti-doublons (1 pseudo = dernier prono) ------------------
def _parse_dt_maybe(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None

def dedupe_pronos_by_playername(items):
    """
    items: liste de dicts contenant au moins:
      - player_name (ou player)
      - payload/p (dict)
      - updated_at (str/iso) optionnel
    Retourne: liste dédoublonnée (1 par pseudo), en gardant le plus récent.
    """
    best = {}
    for it in items or []:
        name = (it.get("player_name") or it.get("player") or "").strip()
        if not name:
            name = "??"
        dt = _parse_dt_maybe(it.get("updated_at")) or _parse_dt_maybe(it.get("_updated_at")) or _parse_dt_maybe(it.get("created_at")) or datetime.min
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

# ✅ NOUVEAU : helper unique (DB + fallback) => {player_name: payload}
def get_latest_pronos_by_player_for_weekend(weekend_id: str) -> dict:
    out = {}

    if engine:
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT DISTINCT ON (player_name)
                    player_name, payload_json, updated_at
                FROM pronos
                WHERE weekend_id=:w
                ORDER BY player_name, updated_at DESC
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


# ================== PUBLIC ROUTES ==================
@app.route("/")
def home():
    name, _ = current_player(request)

    season_year = get_season_year()
    weekends_raw = load_weekends_list()

    weekends = []
    for w in weekends_raw:
        w2 = dict(w)
        w_date = parse_weekend_date(w.get("date", ""), season_year)
        w2["date_obj"] = w_date
        w2["status"] = weekend_status(w_date, open_days_before=10)
        w2["closed_db"] = is_weekend_closed(w.get("id", "")) if engine else False
        weekends.append(w2)

    weekends.sort(key=lambda x: x["date_obj"] or date.max)
    return render_template("index.html", name=name, weekends=weekends, admin_enabled=admin_enabled(), is_admin=is_admin())

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        if not name:
            flash("Entre un pseudo pour continuer.")
            return redirect(url_for("login"))

        _, pid = current_player(request)
        resp = make_response(redirect(url_for("home")))
        resp.set_cookie("player_name", name, max_age=60 * 60 * 24 * 365)
        resp.set_cookie("player_id", pid, max_age=60 * 60 * 24 * 365)
        return resp

    return render_template("login.html", admin_enabled=admin_enabled(), is_admin=is_admin())

@app.route("/logout")
def logout():
    resp = make_response(redirect(url_for("home")))
    resp.delete_cookie("player_name")
    resp.delete_cookie("player_id")
    return resp

# ------------------ Championnat ------------------
@app.route("/championnat", methods=["GET", "POST"])
def championnat():
    name, pid = current_player(request)
    if not name:
        return redirect(url_for("login"))

    my = {}
    if engine:
        with engine.begin() as conn:
            row = conn.execute(text("""
                SELECT payload_json, created_at, updated_at
                FROM championnat_pronos
                WHERE user_key=:u
            """), {"u": pid}).fetchone()
        if row:
            my = dict(row[0] or {})
            my["_created_at"] = str(row[1])
            my["_updated_at"] = str(row[2])
    else:
        all_preds = load_json(championnat_path(), {})
        my = all_preds.get(pid, {})

    if request.method == "POST":
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
                conn.execute(text("""
                    INSERT INTO championnat_pronos (user_key, player_name, payload_json, created_at, updated_at)
                    VALUES (:u, :n, CAST(:p AS jsonb), NOW(), NOW())
                    ON CONFLICT (user_key)
                    DO UPDATE SET
                        player_name = EXCLUDED.player_name,
                        payload_json = EXCLUDED.payload_json,
                        updated_at = NOW()
                """), {
                    "u": pid,
                    "n": name,
                    "p": json.dumps(payload, ensure_ascii=False)
                })
        else:
            all_preds = load_json(championnat_path(), {})
            payload["updated_at"] = datetime.utcnow().isoformat()
            all_preds[pid] = payload
            save_json(championnat_path(), all_preds)

        flash("Pronostic championnat enregistré ✅ (modifiable)")
        return redirect(url_for("championnat"))

    return render_template("championnat.html", riders=RIDERS, my=my, name=name, admin_enabled=admin_enabled(), is_admin=is_admin())

# ------------------ Week-end pronos ------------------
@app.route("/w/<weekend_id>/pronos", methods=["GET", "POST"])
def pronos(weekend_id):
    name, pid = current_player(request)
    if not name:
        return redirect(url_for("login"))

    w = get_weekend(weekend_id)
    if not w:
        return "Week-end inconnu", 404

    closed = is_weekend_closed(weekend_id) if engine else False

    # ✅ si fermé -> redirige vers pronos publics
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
                WHERE weekend_id=:w AND user_key=:u
            """), {"w": weekend_id, "u": pid}).fetchone()
        if row:
            my = dict(row[0] or {})
            my["_created_at"] = str(row[1])
            my["_updated_at"] = str(row[2])
    else:
        all_pronos = load_json(pronos_path(weekend_id), {})
        my = all_pronos.get(pid, {})

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
            "bonus": {b["id"]: form.get(f"bonus_{b['id']}") for b in w.get("bonus_questions", [])}
        }

        if engine:
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO pronos (weekend_id, user_key, player_name, payload_json, created_at, updated_at)
                    VALUES (:w, :u, :n, CAST(:p AS jsonb), NOW(), NOW())
                    ON CONFLICT (weekend_id, user_key)
                    DO UPDATE SET
                        player_name = EXCLUDED.player_name,
                        payload_json = EXCLUDED.payload_json,
                        updated_at = NOW()
                """), {
                    "w": weekend_id,
                    "u": pid,
                    "n": name,
                    "p": json.dumps(payload, ensure_ascii=False)
                })
        else:
            all_pronos = load_json(pronos_path(weekend_id), {})
            payload["updated_at"] = datetime.utcnow().isoformat()
            all_pronos[pid] = payload
            save_json(pronos_path(weekend_id), all_pronos)

        flash("Pronostic enregistré ✅ (modifiable à volonté)")
        return redirect(url_for("pronos", weekend_id=weekend_id))

    return render_template("pronos.html", w=w, riders=RIDERS, my=my, name=name, closed=closed, admin_enabled=admin_enabled(), is_admin=is_admin())

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
    weekends = load_weekends_list()
    enriched = []

    for w in weekends:
        wid = w.get("id")
        if not wid:
            continue

        count = 0
        if engine:
            with engine.begin() as conn:
                row = conn.execute(
                    text("SELECT COUNT(DISTINCT player_name) FROM pronos WHERE weekend_id=:w"),
                    {"w": wid}
                ).fetchone()
            count = int(row[0]) if row else 0
        else:
            all_pronos = load_json(pronos_path(wid), {})
            if isinstance(all_pronos, dict):
                tmp = []
                for _, p in all_pronos.items():
                    tmp.append({
                        "player_name": (p.get("player_name") or "??"),
                        "payload": dict(p or {}),
                        "updated_at": p.get("_updated_at") or p.get("updated_at") or p.get("created_at")
                    })
                count = len(dedupe_pronos_by_playername(tmp))
            else:
                count = 0

        enriched.append({
            "id": wid,
            "label": w.get("label", wid),
            "date": w.get("date"),
            "count": count,
            "closed": is_weekend_closed(wid) if engine else False,
            "public": is_weekend_closed(wid) if engine else False,
        })

    name, _ = current_player(request)
    return render_template("admin_home.html", name=name, weekends=enriched)

@app.route("/admin/w/<weekend_id>")
@require_admin
def admin_weekend(weekend_id):
    w = get_weekend(weekend_id)
    if not w:
        return "Week-end inconnu", 404

    count = 0
    if engine:
        with engine.begin() as conn:
            row = conn.execute(
                text("SELECT COUNT(DISTINCT player_name) FROM pronos WHERE weekend_id=:w"),
                {"w": weekend_id}
            ).fetchone()
        count = int(row[0]) if row else 0
    else:
        all_pronos = load_json(pronos_path(weekend_id), {})
        if isinstance(all_pronos, dict):
            tmp = []
            for _, p in all_pronos.items():
                tmp.append({
                    "player_name": (p.get("player_name") or "??"),
                    "payload": dict(p or {}),
                    "updated_at": p.get("_updated_at") or p.get("updated_at") or p.get("created_at")
                })
            count = len(dedupe_pronos_by_playername(tmp))
        else:
            count = 0

    closed = is_weekend_closed(weekend_id) if engine else False
    public = closed

    name, _ = current_player(request)
    return render_template("admin_weekend.html", name=name, w=w, count=count, closed=closed, public=public)

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

# ------------------ ADMIN : results ------------------
@app.route("/admin/w/<weekend_id>/results", methods=["GET", "POST"])
@require_admin
def admin_results(weekend_id):
    w = get_weekend(weekend_id)
    if not w:
        return "Week-end inconnu", 404

    results = load_results(weekend_id) or {}

    if request.method == "POST":
        f = request.form
        results = {
            "pole": f.get("pole"),
            "q1": [f.get("q1_1"), f.get("q1_2")],
            "sprint": [f.get("sprint_p1"), f.get("sprint_p2"), f.get("sprint_p3")],
            "gp": [f.get("gp_p1"), f.get("gp_p2"), f.get("gp_p3")],
            "bonus": {b["id"]: f.get(f"bonus_{b['id']}") for b in w.get("bonus_questions", [])}
        }
        save_results(weekend_id, results)
        flash("Résultats officiels enregistrés ✅")
        return redirect(url_for("admin_results", weekend_id=weekend_id))

    name, _ = current_player(request)
    return render_template("admin_results.html", name=name, w=w, riders=RIDERS, results=results)

@app.route("/admin/w/<weekend_id>/questions", methods=["GET", "POST"])
@require_admin
def admin_questions(weekend_id):
    data = load_weekends_data()
    weekends = data.get("weekends", [])
    w = None
    for ww in weekends:
        if ww.get("id") == weekend_id:
            w = ww
            break
    if not w:
        return "Week-end inconnu", 404

    w.setdefault("bonus_questions", [])
    by_id = {q.get("id"): q for q in w["bonus_questions"] if isinstance(q, dict)}
    q1 = by_id.get("b1", {"id": "b1", "label": "", "type": "bool"})
    q2 = by_id.get("b2", {"id": "b2", "label": "", "type": "bool"})

    if request.method == "POST":
        q1["label"] = (request.form.get("b1_label") or "").strip()
        q2["label"] = (request.form.get("b2_label") or "").strip()
        q1["type"] = "bool"
        q2["type"] = "bool"

        w["bonus_questions"] = [q1, q2]
        save_json(WEEKENDS_FILE, data)
        flash("Questions bonus enregistrées ✅")
        return redirect(url_for("admin_questions", weekend_id=weekend_id))

    name, _ = current_player(request)
    try:
        return render_template("admin_questions.html", name=name, w=w, q1=q1, q2=q2)
    except Exception:
        return render_template("admin_question.html", name=name, w=w, q1=q1, q2=q2)

# ------------------ Public : page HTML pronos ------------------
@app.route("/w/<weekend_id>/public/pronos")
def public_pronos(weekend_id):
    w = get_weekend(weekend_id)
    if not w:
        return "Week-end inconnu", 404
    if not engine:
        return "DB non configurée (DATABASE_URL manquant).", 500

    if not is_weekend_closed(weekend_id):
        return "Pronos pas encore visibles : le GP est encore ouvert.", 403

    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT ON (player_name)
                player_name, payload_json, updated_at
            FROM pronos
            WHERE weekend_id=:w
            ORDER BY player_name, updated_at DESC
        """), {"w": weekend_id}).fetchall()

    pronos_list = [{
        "player": r[0],
        "updated_at": str(r[2]),
        "p": dict(r[1] or {}),
    } for r in rows]

    pronos_list.sort(key=lambda x: _parse_dt_maybe(x.get("updated_at")) or datetime.min, reverse=True)

    name, _ = current_player(request)
    return render_template(
        "public_pronos.html",
        name=name, w=w, pronos=pronos_list,
        admin_enabled=admin_enabled(), is_admin=is_admin()
    )

# ------------------ Public : Résultats par course ------------------
@app.route("/results_by_race")
def results_by_race():
    name, _ = current_player(request)

    weekends = load_weekends_list()
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
                SELECT DISTINCT ON (player_name)
                    player_name, payload_json, created_at, updated_at
                FROM pronos
                WHERE weekend_id=:w
                ORDER BY player_name, updated_at DESC
            """), {"w": gp_id}).fetchall()

        for r in db_rows:
            pronos_list.append({
                "player_name": r[0],
                "payload": dict(r[1] or {}),
                "created_at": str(r[2]),
                "updated_at": str(r[3]),
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

# ------------------ Classement GP (par week-end) ------------------
@app.route("/w/<weekend_id>/classement")
def classement_weekend(weekend_id):
    w = get_weekend(weekend_id)
    if not w:
        return "Week-end inconnu", 404

    closed = is_weekend_closed(weekend_id) if engine else False
    pronos_by_player = get_latest_pronos_by_player_for_weekend(weekend_id)

    results = load_results(weekend_id)
    if not results:
        name, _ = current_player(request)
        return render_template(
            "classement.html",
            name=name, w=w, rows=[],
            notice="Entre d’abord les résultats officiels (Admin).",
            closed=closed,
            admin_enabled=admin_enabled(), is_admin=is_admin()
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
            well_placed=1.0, mis_placed=0.5, bonus_exact=3.0, bonus_all=1.5
        )

        g_score = podium_points(
            [p.get("gp_p1"), p.get("gp_p2"), p.get("gp_p3")],
            results.get("gp", []),
            well_placed=2.0, mis_placed=1.0, bonus_exact=6.0, bonus_all=3.0
        )

        bonus_score = 0.0
        for b in w.get("bonus_questions", []):
            bid = b.get("id")
            if not bid:
                continue
            pred = (p.get("bonus", {}).get(bid) or "").lower()
            real = (results.get("bonus", {}).get(bid) or "").lower()
            if pred and real and pred == real:
                bonus_score += 0.5

        total = round(q_score + s_score + g_score + bonus_score, 2)
        rows.append({
            "player": player_name or p.get("player_name", "??"),
            "q": q_score,
            "s": s_score,
            "gp": g_score,
            "bonus": bonus_score,
            "total": total
        })

    rows.sort(key=lambda r: r["total"], reverse=True)
    name, _ = current_player(request)
    return render_template(
        "classement.html",
        name=name, w=w, rows=rows, notice=None,
        closed=closed,
        admin_enabled=admin_enabled(), is_admin=is_admin()
    )

# ------------------ ✅ NOUVEAU : Classement général saison ------------------
@app.route("/classement")
def classement_general():
    name, _ = current_player(request)

    weekends = load_weekends_list()
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

    season_year = get_season_year()
    def _w_sort_key(w):
        d = parse_weekend_date(w.get("date", ""), season_year)
        return d or date.max

    weekends_sorted = sorted(weekends, key=_w_sort_key)

    totals = {}
    gp_scored = []

    for w in weekends_sorted:
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
                well_placed=1.0, mis_placed=0.5, bonus_exact=3.0, bonus_all=1.5
            )

            g_score = podium_points(
                [p.get("gp_p1"), p.get("gp_p2"), p.get("gp_p3")],
                results.get("gp", []),
                well_placed=2.0, mis_placed=1.0, bonus_exact=6.0, bonus_all=3.0
            )

            bonus_score = 0.0
            for b in (w.get("bonus_questions", []) or []):
                bid = b.get("id")
                if not bid:
                    continue
                pred = (p.get("bonus", {}).get(bid) or "").lower()
                real = (results.get("bonus", {}).get(bid) or "").lower()
                if pred and real and pred == real:
                    bonus_score += 0.5

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
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except Exception as e:
            return f"DB ERROR: {e}", 500

    resp = make_response("OK", 200)
    resp.headers["Cache-Control"] = "no-store"
    return resp