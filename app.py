import os
import json
import uuid
from datetime import datetime, date, timedelta
from flask import Flask, render_template, request, redirect, url_for, make_response, flash

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")
app.permanent_session_lifetime = timedelta(days=365)

# 🔐 Clé admin (à définir sur Render)
ADMIN_KEY = os.environ.get("ADMIN_KEY", "")

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

# ------------------ JSON helpers ------------------

def load_json(path, default):
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except:
            return default

def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ------------------ Weekends ------------------

def load_weekends_data():
    return load_json(WEEKENDS_FILE, {"season_year": 2026, "weekends": []})

def load_weekends_list():
    return load_weekends_data().get("weekends", [])

def get_weekend(weekend_id):
    for w in load_weekends_list():
        if w.get("id") == weekend_id:
            w.setdefault("bonus_questions", [])
            return w
    return None

def parse_weekend_date(raw_date, season_year):
    try:
        return datetime.strptime(raw_date, "%Y-%m-%d").date()
    except:
        return None

def weekend_status(weekend_date, open_days_before=10):
    if not weekend_date:
        return "closed"

    today = date.today()

    if weekend_date < today:
        return "past"

    open_from = weekend_date - timedelta(days=open_days_before)

    if today >= open_from:
        return "open"

    return "closed"

# ------------------ Identification joueur ------------------

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
    a = [normalize(x) for x in actual]

    score = 0.0

    for i in range(3):
        if i < len(a) and p[i] == a[i]:
            score += well_placed
        elif p[i] in a:
            score += mis_placed

    if len(a) >= 3 and p == a:
        score += bonus_exact
    elif set(p) == set(a):
        score += bonus_all

    return score

def qualif_points(pole_pred, pole_real, q1_preds, q1_actual):
    score = 0.0

    if normalize(pole_pred) == normalize(pole_real):
        score += 2.0

    real_set = {normalize(x) for x in q1_actual or []}

    for p in q1_preds or []:
        if normalize(p) in real_set:
            score += 0.5

    return score

# ------------------ Paths ------------------

def pronos_path(weekend_id):
    return os.path.join(PRONOS_DIR, f"{weekend_id}.json")

def results_path(weekend_id):
    return os.path.join(RESULTS_DIR, f"{weekend_id}.json")

# ------------------ Routes ------------------

@app.route("/")
def home():
    name, _ = current_player(request)

    data = load_weekends_data()
    season_year = data.get("season_year", 2026)
    weekends_raw = data.get("weekends", [])

    weekends = []

    for w in weekends_raw:
        w2 = dict(w)
        w_date = parse_weekend_date(w.get("date"), season_year)
        w2["date_obj"] = w_date
        w2["status"] = weekend_status(w_date)
        weekends.append(w2)

    weekends.sort(key=lambda x: x["date_obj"] or date.max)

    return render_template("index.html", name=name, weekends=weekends)

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        if not name:
            flash("Entre un pseudo.")
            return redirect(url_for("login"))

        _, pid = current_player(request)
        resp = make_response(redirect(url_for("home")))
        resp.set_cookie("player_name", name, max_age=60*60*24*365)
        resp.set_cookie("player_id", pid, max_age=60*60*24*365)
        return resp

    return render_template("login.html")

@app.route("/logout")
def logout():
    resp = make_response(redirect(url_for("home")))
    resp.delete_cookie("player_name")
    resp.delete_cookie("player_id")
    return resp

@app.route("/w/<weekend_id>/pronos", methods=["GET", "POST"])
def pronos(weekend_id):
    name, pid = current_player(request)
    if not name:
        return redirect(url_for("login"))

    w = get_weekend(weekend_id)
    if not w:
        return "Week-end inconnu", 404

    all_pronos = load_json(pronos_path(weekend_id), {})
    my = all_pronos.get(pid, {})

    if request.method == "POST":
        form = request.form

        my = {
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

        all_pronos[pid] = my
        save_json(pronos_path(weekend_id), all_pronos)

        flash("Pronostic enregistré ✅")
        return redirect(url_for("pronos", weekend_id=weekend_id))

    return render_template("pronos.html", w=w, riders=RIDERS, my=my, name=name)

# ------------------ ADMIN RESULTS ------------------

@app.route("/w/<weekend_id>/admin/results", methods=["GET", "POST"])
def admin_results(weekend_id):

    provided = request.args.get("key", "")
    if ADMIN_KEY and provided != ADMIN_KEY:
        return "Accès admin refusé", 403

    w = get_weekend(weekend_id)
    if not w:
        return "Week-end inconnu", 404

    results = load_json(results_path(weekend_id), {})

    if request.method == "POST":
        f = request.form

        results = {
            "pole": f.get("pole"),
            "q1": [f.get("q1_1"), f.get("q1_2")],
            "sprint": [f.get("sprint_p1"), f.get("sprint_p2"), f.get("sprint_p3")],
            "gp": [f.get("gp_p1"), f.get("gp_p2"), f.get("gp_p3")],
            "bonus": {b["id"]: f.get(f"bonus_{b['id']}") for b in w.get("bonus_questions", [])}
        }

        save_json(results_path(weekend_id), results)
        flash("Résultats enregistrés ✅")

        return redirect(url_for("admin_results", weekend_id=weekend_id, key=provided))

    return render_template("admin_results.html", w=w, riders=RIDERS, results=results)

@app.route("/w/<weekend_id>/classement")
def classement_weekend(weekend_id):

    w = get_weekend(weekend_id)
    if not w:
        return "Week-end inconnu", 404

    all_pronos = load_json(pronos_path(weekend_id), {})
    results = load_json(results_path(weekend_id), None)

    if not results:
        return render_template("classement.html", w=w, rows=[], notice="Résultats non saisis.")

    rows = []

    for pid, p in all_pronos.items():

        q_score = qualif_points(
            p.get("pole"),
            results.get("pole"),
            [p.get("q1_1"), p.get("q1_2")],
            results.get("q1", []),
        )

        s_score = podium_points(
            [p.get("sprint_p1"), p.get("sprint_p2"), p.get("sprint_p3")],
            results.get("sprint", []),
            1.0, 0.5, 3.0, 1.5
        )

        g_score = podium_points(
            [p.get("gp_p1"), p.get("gp_p2"), p.get("gp_p3")],
            results.get("gp", []),
            2.0, 1.0, 6.0, 3.0
        )

        bonus_score = 0.0

        for b in w.get("bonus_questions", []):
            pred = (p.get("bonus", {}).get(b["id"]) or "").lower()
            real = (results.get("bonus", {}).get(b["id"]) or "").lower()
            if pred and real and pred == real:
                bonus_score += 0.5

        total = round(q_score + s_score + g_score + bonus_score, 2)

        rows.append({
            "player": p.get("player_name", "??"),
            "total": total
        })

    rows.sort(key=lambda r: r["total"], reverse=True)

    return render_template("classement.html", w=w, rows=rows, notice=None)

@app.route("/health")
def health():
    return "OK", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)    "#12 Maverick Vinales",
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

# ------------------ JSON helpers ------------------
def load_json(path, default):
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except:
            return default

def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ------------------ Weekends helpers ------------------
def load_weekends_data():
    """
    Accepte 2 formats :
    - ancien : [ {id, label, ...}, ... ]
    - nouveau : { season_year, timezone, weekends: [ ... ] }
    Retourne TOUJOURS : { "weekends": [...] }
    """
    data = load_json(WEEKENDS_FILE, {"weekends": []})

    if isinstance(data, list):
        return {"weekends": data}

    if isinstance(data, dict) and "weekends" in data:
        return data

    return {"weekends": []}

def load_weekends_list():
    return load_weekends_data().get("weekends", [])

def save_weekends_list(new_list):
    """Sauvegarde la liste 'weekends' en conservant les champs season_year/timezone si existants."""
    data = load_weekends_data()
    if not isinstance(data, dict):
        data = {"weekends": []}
    data["weekends"] = new_list
    save_json(WEEKENDS_FILE, data)

def bootstrap_weekends():
    # Si ton fichier weekends.json existe déjà (ton vrai calendrier), on ne touche à rien
    if os.path.exists(WEEKENDS_FILE):
        return

    # Sinon on crée un petit week-end de démo
    demo = {
        "season_year": 2026,
        "timezone": "Europe/Paris",
        "weekends": [
            {
                "id": "qatar",
                "label": "GP du Qatar",
                "date": "04-12",   # format MM-DD
                "time": None,
                "bonus_questions": [
                    {"id": "b1", "label": "Un pilote Ducati sur le podium du GP ?", "type": "bool"},
                    {"id": "b2", "label": "Chute lors du sprint ?", "type": "bool"}
                ]
            }
        ]
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

# ------------------ Statuts GP (past/closed/open) ------------------
def get_season_year():
    data = load_weekends_data()
    y = data.get("season_year")
    if isinstance(y, int):
        return y
    return date.today().year

def parse_weekend_date(raw_date: str, season_year: int):
    """
    Supporte :
    - "MM-DD" (ex: "04-12") -> date(season_year, 4, 12)
    - "YYYY-MM-DD" (ex: "2026-04-12")
    Retourne None si format invalide.
    """
    raw_date = (raw_date or "").strip()
    if not raw_date:
        return None

    # cas "YYYY-MM-DD"
    try:
        return datetime.strptime(raw_date, "%Y-%m-%d").date()
    except Exception:
        pass

    # cas "MM-DD"
    try:
        mm, dd = raw_date.split("-")
        return date(season_year, int(mm), int(dd))
    except Exception:
        return None

def weekend_status(weekend_date: date, open_days_before: int = 10):
    """
    past : week-end passé
    open : ouverture dans une fenêtre (par défaut 10 jours avant la date)
    closed : futur mais pas encore ouvert
    """
    if not weekend_date:
        return "closed"

    today = date.today()
    if weekend_date < today:
        return "past"

    open_from = weekend_date - timedelta(days=open_days_before)
    if today >= open_from:
        return "open"

    return "closed"

# ------------------ Identification simple (cookies) ------------------
def current_player(req):
    name = req.cookies.get("player_name")
    pid = req.cookies.get("player_id")
    if not pid:
        pid = str(uuid.uuid4())
    return name, pid

# ------------------ Admin helpers ------------------
def is_admin():
    return session.get("is_admin") is True

def admin_required():
    if not is_admin():
        abort(403)

# ------------------ Règles de points ------------------
def normalize(x):
    return (x or "").strip().lower()

def podium_points(pred, actual, well_placed, mis_placed, bonus_exact, bonus_all):
    p = [normalize(x) for x in pred]
    a = [normalize(x) for x in actual]

    score = 0.0
    # bien placés
    for i in range(3):
        if p[i] and i < len(a) and p[i] == a[i]:
            score += well_placed
    # mal placés
    for i in range(3):
        if p[i] and p[i] in a and (i >= len(a) or p[i] != a[i]):
            score += mis_placed

    # bonus
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

    return score  # max 3

# ------------------ Fichiers de pronos & résultats ------------------
def pronos_path(weekend_id):
    return os.path.join(PRONOS_DIR, f"{weekend_id}.json")

def results_path(weekend_id):
    return os.path.join(RESULTS_DIR, f"{weekend_id}.json")

# ------------------ Routes “site” ------------------
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
        weekends.append(w2)

    weekends.sort(key=lambda x: x["date_obj"] or date.max)

    return render_template("index.html", name=name, weekends=weekends)

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        if not name:
            flash("Entre un pseudo pour continuer.")
            return redirect(url_for("login"))

        _, pid = current_player(request)
        resp = make_response(redirect(url_for("home")))
        resp.set_cookie("player_name", name, max_age=60 * 60 * 24 * 365)
        resp.set_cookie("player_id", pid, max_age=60 * 60 * 24 * 365)
        return resp

    return render_template("login.html")

@app.route("/logout")
def logout():
    resp = make_response(redirect(url_for("home")))
    resp.delete_cookie("player_name")
    resp.delete_cookie("player_id")
    return resp

@app.route("/w/<weekend_id>/pronos", methods=["GET", "POST"])
def pronos(weekend_id):
    name, pid = current_player(request)
    if not name:
        return redirect(url_for("login"))

    w = get_weekend(weekend_id)
    if not w:
        return "Week-end inconnu", 404

    all_pronos = load_json(pronos_path(weekend_id), {})
    my = all_pronos.get(pid, {})

    if request.method == "POST":
        form = request.form

        def has_duplicates(values):
            v = [x for x in values if x]
            return len(set(v)) != len(v)

        if has_duplicates([form.get("q1_1"), form.get("q1_2")]) \
           or has_duplicates([form.get("sprint_p1"), form.get("sprint_p2"), form.get("sprint_p3")]) \
           or has_duplicates([form.get("gp_p1"), form.get("gp_p2"), form.get("gp_p3")]):
            flash("Doublon détecté : un pilote ne peut apparaître qu'une fois dans Q1 / Sprint / GP.")
            return redirect(url_for("pronos", weekend_id=weekend_id))

        my = {
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
        all_pronos[pid] = my
        save_json(pronos_path(weekend_id), all_pronos)
        flash("Pronostic enregistré ✅ (modifiable à volonté)")
        return redirect(url_for("pronos", weekend_id=weekend_id))

    return render_template("pronos.html", w=w, riders=RIDERS, my=my, name=name)

# ------------------ Admin: login + dashboard + bonus ------------------
@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if request.method == "POST":
        key = (request.form.get("key") or "").strip()

        if ADMIN_KEY and key == ADMIN_KEY:
            session["is_admin"] = True
            session.permanent = True
            app.permanent_session_lifetime = timedelta(days=ADMIN_SESSION_DAYS)
            flash("Admin connecté ✅")
            return redirect(url_for("admin_dashboard"))

        flash("Clé admin incorrecte ❌")
        return redirect(url_for("admin_login"))

    return render_template("admin_login.html")

@app.route("/admin/logout")
def admin_logout():
    session.pop("is_admin", None)
    flash("Admin déconnecté.")
    return redirect(url_for("home"))

@app.route("/admin")
def admin_dashboard():
    admin_required()

    season_year = get_season_year()
    weekends_raw = load_weekends_list()

    rows = []
    for w in weekends_raw:
        wid = w.get("id")
        w_date = parse_weekend_date(w.get("date", ""), season_year)
        status = weekend_status(w_date, open_days_before=10)

        pronos_data = load_json(pronos_path(wid), {})
        results_data = load_json(results_path(wid), None)

        rows.append({
            "id": wid,
            "label": w.get("label", wid),
            "date": w.get("date", ""),
            "status": status,
            "pronos_count": len(pronos_data or {}),
            "has_results": bool(results_data),
            "bonus_count": len(w.get("bonus_questions", []) or []),
        })

    rows.sort(key=lambda r: r["date"] or "9999-99-99")
    return render_template("admin_dashboard.html", rows=rows)

@app.route("/admin/w/<weekend_id>/bonus", methods=["GET", "POST"])
def admin_bonus(weekend_id):
    admin_required()

    weekends = load_weekends_list()
    w = None
    for item in weekends:
        if item.get("id") == weekend_id:
            w = item
            break

    if not w:
        return "Week-end inconnu", 404

    w.setdefault("bonus_questions", [])

    if request.method == "POST":
        action = (request.form.get("action") or "").strip()

        if action == "add":
            label = (request.form.get("label") or "").strip()
            if not label:
                flash("Le libellé est obligatoire.")
                return redirect(url_for("admin_bonus", weekend_id=weekend_id))

            # id unique simple
            new_id = f"b{len(w['bonus_questions']) + 1}"
            existing_ids = {q.get("id") for q in w["bonus_questions"]}
            while new_id in existing_ids:
                new_id = f"b{uuid.uuid4().hex[:6]}"

            w["bonus_questions"].append({
                "id": new_id,
                "label": label,
                "type": "bool"
            })
            save_weekends_list(weekends)
            flash("Question bonus ajoutée ✅")
            return redirect(url_for("admin_bonus", weekend_id=weekend_id))

        if action == "delete":
            qid = (request.form.get("qid") or "").strip()
            before = len(w["bonus_questions"])
            w["bonus_questions"] = [q for q in w["bonus_questions"] if q.get("id") != qid]
            after = len(w["bonus_questions"])
            save_weekends_list(weekends)
            flash("Question bonus supprimée ✅" if after < before else "Question introuvable.")
            return redirect(url_for("admin_bonus", weekend_id=weekend_id))

    return render_template("admin_bonus.html", w=w)

# ------------------ Admin: résultats (protégé) ------------------
@app.route("/w/<weekend_id>/admin/results", methods=["GET", "POST"])
def admin_results(weekend_id):
    admin_required()

    w = get_weekend(weekend_id)
    if not w:
        return "Week-end inconnu", 404

    results = load_json(results_path(weekend_id), {})
    if request.method == "POST":
        f = request.form
        results = {
            "pole": f.get("pole"),
            "q1": [f.get("q1_1"), f.get("q1_2")],
            "sprint": [f.get("sprint_p1"), f.get("sprint_p2"), f.get("sprint_p3")],
            "gp": [f.get("gp_p1"), f.get("gp_p2"), f.get("gp_p3")],
            "bonus": {b["id"]: f.get(f"bonus_{b['id']}") for b in w.get("bonus_questions", [])}
        }
        save_json(results_path(weekend_id), results)
        flash("Résultats officiels enregistrés ✅")
        return redirect(url_for("admin_results", weekend_id=weekend_id))

    return render_template("admin_results.html", w=w, riders=RIDERS, results=results)

# ------------------ Classement GP ------------------
@app.route("/w/<weekend_id>/classement")
def classement_weekend(weekend_id):
    w = get_weekend(weekend_id)
    if not w:
        return "Week-end inconnu", 404

    all_pronos = load_json(pronos_path(weekend_id), {})
    results = load_json(results_path(weekend_id), None)
    if not results:
        return render_template("classement.html", w=w, rows=[], notice="Entre d’abord les résultats officiels (Admin).")

    rows = []
    for pid, p in all_pronos.items():
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
            pred = (p.get("bonus", {}).get(b["id"]) or "").lower()
            real = (results.get("bonus", {}).get(b["id"]) or "").lower()
            if pred and real and pred == real:
                bonus_score += 0.5

        total = round(q_score + s_score + g_score + bonus_score, 2)
        rows.append({
            "player": p.get("player_name", "??"),
            "q": q_score,
            "s": s_score,
            "gp": g_score,
            "bonus": bonus_score,
            "total": total
        })

    rows.sort(key=lambda r: r["total"], reverse=True)
    return render_template("classement.html", w=w, rows=rows, notice=None)

# ------------------ Healthcheck (pour ping anti-sleep) ------------------
@app.route("/health")
def health():
    return "OK", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)    "#63 Francesco Bagnaia",
    "#72 Marco Bezzecchi",
    "#73 Alex Marquez",
    "#79 Ai Ogura",
    "#89 Jorge Martin",
    "#93 Marc Marquez",
]

# ------------------ JSON helpers ------------------
def load_json(path, default):
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except:
            return default

def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ------------------ Weekends helpers ------------------
def load_weekends_data():
    """
    Accepte 2 formats :
    - ancien : [ {id, label, ...}, ... ]
    - nouveau : { season_year, timezone, weekends: [ ... ] }
    Retourne TOUJOURS : { "weekends": [...] }
    """
    data = load_json(WEEKENDS_FILE, {"weekends": []})

    if isinstance(data, list):
        return {"weekends": data}

    if isinstance(data, dict) and "weekends" in data:
        return data

    return {"weekends": []}

def load_weekends_list():
    return load_weekends_data().get("weekends", [])

def bootstrap_weekends():
    # Si ton fichier weekends.json existe déjà (ton vrai calendrier), on ne touche à rien
    if os.path.exists(WEEKENDS_FILE):
        return

    # Sinon on crée un petit week-end de démo
    demo = {
        "season_year": 2026,
        "timezone": "Europe/Paris",
        "weekends": [
            {
                "id": "qatar",
                "label": "GP du Qatar",
                "date": "04-12",   # format MM-DD
                "time": None,
                "bonus_questions": [
                    {"id": "b1", "label": "Un pilote Ducati sur le podium du GP ?", "type": "bool"},
                    {"id": "b2", "label": "Chute lors du sprint ?", "type": "bool"}
                ]
            }
        ]
    }
    os.makedirs(DATA_DIR, exist_ok=True)
    save_json(WEEKENDS_FILE, demo)

bootstrap_weekends()

def get_weekend(weekend_id):
    for w in load_weekends_list():
        if w.get("id") == weekend_id:
            # assure que bonus_questions existe toujours
            w.setdefault("bonus_questions", [])
            return w
    return None

# ------------------ Statuts GP (past/closed/open) ------------------
def get_season_year():
    data = load_weekends_data()
    y = data.get("season_year")
    if isinstance(y, int):
        return y
    return date.today().year

def parse_weekend_date(raw_date: str, season_year: int):
    """
    Supporte :
    - "MM-DD" (ex: "04-12") -> date(season_year, 4, 12)
    - "YYYY-MM-DD" (ex: "2026-04-12")
    Retourne None si format invalide.
    """
    raw_date = (raw_date or "").strip()
    if not raw_date:
        return None

    # cas "YYYY-MM-DD"
    try:
        return datetime.strptime(raw_date, "%Y-%m-%d").date()
    except Exception:
        pass

    # cas "MM-DD"
    try:
        mm, dd = raw_date.split("-")
        return date(season_year, int(mm), int(dd))
    except Exception:
        return None

def weekend_status(weekend_date: date, open_days_before: int = 10):
    """
    past : week-end passé
    open : ouverture dans une fenêtre (par défaut 10 jours avant la date)
    closed : futur mais pas encore ouvert
    """
    if not weekend_date:
        return "closed"  # par défaut, on considère non ouvert si date inconnue

    today = date.today()
    if weekend_date < today:
        return "past"

    open_from = weekend_date - timedelta(days=open_days_before)
    if today >= open_from:
        return "open"

    return "closed"

# ------------------ Identification simple (cookies) ------------------
def current_player(req):
    name = req.cookies.get("player_name")
    pid = req.cookies.get("player_id")
    if not pid:
        pid = str(uuid.uuid4())
    return name, pid

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        if not name:
            flash("Entre un pseudo pour continuer.")
            return redirect(url_for("login"))

        _, pid = current_player(request)
        resp = make_response(redirect(url_for("home")))
        resp.set_cookie("player_name", name, max_age=60 * 60 * 24 * 365)
        resp.set_cookie("player_id", pid, max_age=60 * 60 * 24 * 365)
        return resp

    return render_template("login.html")

@app.route("/logout")
def logout():
    resp = make_response(redirect(url_for("home")))
    resp.delete_cookie("player_name")
    resp.delete_cookie("player_id")
    return resp

# ------------------ Règles de points ------------------
def normalize(x):
    return (x or "").strip().lower()

def podium_points(pred, actual, well_placed, mis_placed, bonus_exact, bonus_all):
    p = [normalize(x) for x in pred]
    a = [normalize(x) for x in actual]

    score = 0.0
    # bien placés
    for i in range(3):
        if p[i] and i < len(a) and p[i] == a[i]:
            score += well_placed
    # mal placés
    for i in range(3):
        if p[i] and p[i] in a and (i >= len(a) or p[i] != a[i]):
            score += mis_placed

    # bonus
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

    return score  # max 3

# ------------------ Fichiers de pronos & résultats ------------------
def pronos_path(weekend_id):
    return os.path.join(PRONOS_DIR, f"{weekend_id}.json")

def results_path(weekend_id):
    return os.path.join(RESULTS_DIR, f"{weekend_id}.json")

# ------------------ Routes ------------------
@app.route("/")
def home():
    name, _ = current_player(request)

    season_year = get_season_year()
    weekends_raw = load_weekends_list()

    weekends = []
    for w in weekends_raw:
        w2 = dict(w)  # copie pour ne pas modifier l'original
        w_date = parse_weekend_date(w.get("date", ""), season_year)
        w2["date_obj"] = w_date
        w2["status"] = weekend_status(w_date, open_days_before=10)
        weekends.append(w2)

    weekends.sort(key=lambda x: x["date_obj"] or date.max)

    return render_template("index.html", name=name, weekends=weekends)

@app.route("/w/<weekend_id>/pronos", methods=["GET", "POST"])
def pronos(weekend_id):
    name, pid = current_player(request)
    if not name:
        return redirect(url_for("login"))

    w = get_weekend(weekend_id)
    if not w:
        return "Week-end inconnu", 404

    all_pronos = load_json(pronos_path(weekend_id), {})
    my = all_pronos.get(pid, {})

    if request.method == "POST":
        form = request.form

        # --- Anti-doublons (sécurité serveur) ---
        def has_duplicates(values):
            v = [x for x in values if x]
            return len(set(v)) != len(v)

        if has_duplicates([form.get("q1_1"), form.get("q1_2")]) \
           or has_duplicates([form.get("sprint_p1"), form.get("sprint_p2"), form.get("sprint_p3")]) \
           or has_duplicates([form.get("gp_p1"), form.get("gp_p2"), form.get("gp_p3")]):
            flash("Doublon détecté : un pilote ne peut apparaître qu'une fois dans Q1 / Sprint / GP.")
            return redirect(url_for("pronos", weekend_id=weekend_id))

        my = {
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
        all_pronos[pid] = my
        save_json(pronos_path(weekend_id), all_pronos)
        flash("Pronostic enregistré ✅ (modifiable à volonté)")
        return redirect(url_for("pronos", weekend_id=weekend_id))

    return render_template("pronos.html", w=w, riders=RIDERS, my=my, name=name)

@app.route("/w/<weekend_id>/admin/results", methods=["GET", "POST"])
def admin_results(weekend_id):
        # 🔐 Protection admin
    provided = request.args.get("key", "")
    if ADMIN_KEY and provided != ADMIN_KEY:
        return "Accès admin refusé", 403
    w = get_weekend(weekend_id)
    if not w:
        return "Week-end inconnu", 404

    results = load_json(results_path(weekend_id), {})
    if request.method == "POST":
        f = request.form
        results = {
            "pole": f.get("pole"),
            "q1": [f.get("q1_1"), f.get("q1_2")],
            "sprint": [f.get("sprint_p1"), f.get("sprint_p2"), f.get("sprint_p3")],
            "gp": [f.get("gp_p1"), f.get("gp_p2"), f.get("gp_p3")],
            "bonus": {b["id"]: f.get(f"bonus_{b['id']}") for b in w.get("bonus_questions", [])}
        }
        save_json(results_path(weekend_id), results)
        flash("Résultats officiels enregistrés ✅")
        return redirect(url_for("admin_results", weekend_id=weekend_id))

    return render_template("admin_results.html", w=w, riders=RIDERS, results=results)

@app.route("/w/<weekend_id>/classement")
def classement_weekend(weekend_id):
    w = get_weekend(weekend_id)
    if not w:
        return "Week-end inconnu", 404

    all_pronos = load_json(pronos_path(weekend_id), {})
    results = load_json(results_path(weekend_id), None)
    if not results:
        return render_template("classement.html", w=w, rows=[], notice="Entre d’abord les résultats officiels (menu Admin).")

    rows = []
    for pid, p in all_pronos.items():
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
            pred = (p.get("bonus", {}).get(b["id"]) or "").lower()
            real = (results.get("bonus", {}).get(b["id"]) or "").lower()
            if pred and real and pred == real:
                bonus_score += 0.5

        total = round(q_score + s_score + g_score + bonus_score, 2)
        rows.append({
            "player": p.get("player_name", "??"),
            "q": q_score,
            "s": s_score,
            "gp": g_score,
            "bonus": bonus_score,
            "total": total
        })

    rows.sort(key=lambda r: r["total"], reverse=True)
    return render_template("classement.html", w=w, rows=rows, notice=None)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
