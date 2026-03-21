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
# + NOUVEAU : suppression admin d'un prono joueur par GP

import os
import json
import uuid
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
PARTICIPANTS_FILE = os.path.join(DATA_DIR, "participants.json")

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

# ------------------ Utils ------------------
def normalize_pseudo(pseudo: str) -> str:
    return " ".join((pseudo or "").strip().lower().split())


def _parse_dt_maybe(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


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

        conn.execute(text("ALTER TABLE weekends ADD COLUMN IF NOT EXISTS pronos_public_at TIMESTAMPTZ NULL;"))
        conn.execute(text("ALTER TABLE weekends ADD COLUMN IF NOT EXISTS results_published_at TIMESTAMPTZ NULL;"))

        # Participants autorisés
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS participants (
            id SERIAL PRIMARY KEY,
            pseudo TEXT NOT NULL,
            pseudo_normalized TEXT NOT NULL,
            active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """))
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_participants_pseudo_normalized ON participants(pseudo_normalized);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_participants_active ON participants(active);"))

        # Pronos GP
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

        # Colonnes ajoutées pour la nouvelle logique
        conn.execute(text("ALTER TABLE pronos ADD COLUMN IF NOT EXISTS player_norm TEXT NULL;"))
        conn.execute(text("ALTER TABLE pronos ADD COLUMN IF NOT EXISTS participant_id INTEGER NULL;"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_pronos_weekend ON pronos(weekend_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_pronos_weekend_player ON pronos(weekend_id, player_name);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_pronos_weekend_player_norm ON pronos(weekend_id, player_norm);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_pronos_participant_id ON pronos(participant_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_pronos_updated_at ON pronos(updated_at);"))

        # Championnat
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
        conn.execute(text("ALTER TABLE championnat_pronos ADD COLUMN IF NOT EXISTS player_norm TEXT NULL;"))
        conn.execute(text("ALTER TABLE championnat_pronos ADD COLUMN IF NOT EXISTS participant_id INTEGER NULL;"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_championnat_pronos_player_norm ON championnat_pronos(player_norm);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_championnat_pronos_participant_id ON championnat_pronos(participant_id);"))

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

        # Backfill player_norm pour anciennes lignes
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


db_init()

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
        with engine.begin() as conn:
            if include_inactive:
                rows = conn.execute(text("""
                    SELECT id, pseudo, pseudo_normalized, active, created_at
                    FROM participants
                    ORDER BY pseudo ASC
                """)).fetchall()
            else:
                rows = conn.execute(text("""
                    SELECT id, pseudo, pseudo_normalized, active, created_at
                    FROM participants
                    WHERE active = TRUE
                    ORDER BY pseudo ASC
                """)).fetchall()

        return [{
            "id": r[0],
            "pseudo": r[1],
            "pseudo_normalized": r[2],
            "active": bool(r[3]),
            "created_at": str(r[4]) if r[4] else None
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
        with engine.begin() as conn:
            if active_only:
                row = conn.execute(text("""
                    SELECT id, pseudo, pseudo_normalized, active, created_at
                    FROM participants
                    WHERE pseudo_normalized=:pn AND active=TRUE
                    LIMIT 1
                """), {"pn": pseudo_norm}).fetchone()
            else:
                row = conn.execute(text("""
                    SELECT id, pseudo, pseudo_normalized, active, created_at
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
            "active": bool(row[3]),
            "created_at": str(row[4]) if row[4] else None
        }

    for p in get_all_participants(include_inactive=not active_only):
        if p["pseudo_normalized"] == pseudo_norm:
            return p
    return None


def add_participant(pseudo: str):
    pseudo = (pseudo or "").strip()
    pseudo_norm = normalize_pseudo(pseudo)
    if not pseudo or not pseudo_norm:
        return False, "Le pseudo est obligatoire."

    existing = get_participant_by_input(pseudo, active_only=False)
    if existing:
        return False, "Ce pseudo existe déjà."

    if engine:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO participants (pseudo, pseudo_normalized, active, created_at)
                VALUES (:p, :pn, TRUE, NOW())
            """), {"p": pseudo, "pn": pseudo_norm})
        return True, "Joueur ajouté."

    items = load_participants_fallback()
    items.append({
        "id": len(items) + 1,
        "pseudo": pseudo,
        "pseudo_normalized": pseudo_norm,
        "active":