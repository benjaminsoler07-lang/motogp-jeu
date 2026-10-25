"""PronoGP V2: fonctions additives (profil, calendrier de séances, espace joueur).
Aucune modification destructive des tables de pronostics existantes.
"""
from __future__ import annotations

import io
import re
import secrets
import hmac
import json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from flask import request, session, flash, redirect, url_for, render_template, abort, Response, send_file
from sqlalchemy import text
from PIL import Image, ImageOps, UnidentifiedImageError

PARIS = ZoneInfo("Europe/Paris")
UTC = timezone.utc
MAX_PHOTO_SIZE = 10 * 1024 * 1024
SESSION_LABELS = ("FP1", "Essais", "Practice", "FP2", "Q1", "Q2", "Sprint", "Warm-up", "Course")


def deadline_from_weekend(weekend):
    value = str((weekend or {}).get("deadline_utc") or "")
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.astimezone(UTC) if dt.tzinfo else None
    except ValueError:
        return None


def parse_paris_local(raw):
    if not raw:
        return None
    dt = datetime.strptime(raw, "%Y-%m-%dT%H:%M")
    aware = dt.replace(tzinfo=PARIS)
    # DST: les heures inexistantes sont rejetées, les heures ambigües utilisent fold=0.
    if aware.astimezone(UTC).astimezone(PARIS).replace(tzinfo=None) != dt:
        raise ValueError("Horaire inexistant lors du changement d'heure.")
    return aware.astimezone(UTC)


def paris_input(utc_iso):
    if not utc_iso:
        return ""
    try:
        parsed = datetime.fromisoformat(str(utc_iso).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return ""
        return parsed.astimezone(PARIS).strftime("%Y-%m-%dT%H:%M")
    except ValueError:
        return ""


def race_center(weekends):
    now = datetime.now(UTC)
    upcoming = []
    for weekend in weekends:
        if weekend.get("cancelled"):
            continue
        deadline = deadline_from_weekend(weekend)
        sessions = []
        for s in weekend.get("sessions", []):
            if not isinstance(s, dict):
                continue
            try:
                starts = datetime.fromisoformat(str(s["utc"]).replace("Z", "+00:00"))
                if starts.tzinfo and starts > now:
                    sessions.append((starts, str(s.get("label", ""))))
            except (ValueError, KeyError, TypeError):
                continue
        sessions.sort()
        next_session = sessions[0] if sessions else None
        if deadline and deadline > now:
            upcoming.append((deadline, weekend, next_session))
        elif next_session:
            upcoming.append((next_session[0], weekend, next_session))
    if not upcoming:
        return None
    upcoming.sort(key=lambda value: value[0])
    _, weekend, session_next = upcoming[0]
    deadline = deadline_from_weekend(weekend)
    return {
        "id": weekend["id"],
        "name": weekend.get("label", ""),
        "deadline_iso": deadline.isoformat() if deadline else None,
        "session_label": session_next[1] if session_next else None,
        "session_iso": session_next[0].isoformat() if session_next else None,
    }


def register_v2(app, api):
    engine = api["engine"]

    @app.context_processor
    def inject_v2_avatars():
        try:
            players = api["get_all_participants"](include_inactive=True)
            return {"v2_avatar_ids": {
                api["normalize_pseudo"](p["pseudo"]): p["id"]
                for p in players if p.get("id") and p.get("pseudo")
            }}
        except Exception:
            app.logger.exception("Unable to load V2 avatar identifiers")
            return {"v2_avatar_ids": {}}

    def connected():
        return api["get_current_participant"](request)

    def admin_only():
        if not api["is_admin"]():
            abort(403)

    def csrf_value():
        if not session.get("v2_csrf"):
            session["v2_csrf"] = secrets.token_urlsafe(32)
        return session["v2_csrf"]

    def check_csrf():
        if not hmac.compare_digest(
            str(request.form.get("csrf") or ""), str(csrf_value())):
            abort(403)

    @app.route("/admin/w/<weekend_id>/schedule", methods=["GET", "POST"])
    def admin_schedule(weekend_id):
        admin_only()
        weekend = api["get_weekend"](weekend_id)
        if not weekend:
            abort(404)
        if request.method == "POST":
            check_csrf()
            try:
                deadline_raw = (request.form.get("deadline") or "").strip()
                deadline = parse_paris_local(deadline_raw) if deadline_raw else None
                lines = (request.form.get("sessions") or "").splitlines()
                if len(lines) > 20:
                    raise ValueError("20 séances maximum.")
                sessions = []
                for line in lines:
                    if not line.strip():
                        continue
                    label, sep, time_value = line.partition("|")
                    label = label.strip()
                    if not sep or not label or len(label) > 50:
                        raise ValueError("Séance attendue sous la forme Q1|2026-10-25T10:30.")
                    when = parse_paris_local(time_value.strip())
                    if not when:
                        raise ValueError("Horaire de séance obligatoire.")
                    sessions.append({"label": label, "utc": when.isoformat()})
                sessions.sort(key=lambda row: row["utc"])
                data = api["load_weekends_data"]()
                for row in data["weekends"]:
                    if row.get("id") == weekend_id:
                        row["deadline_utc"] = deadline.isoformat() if deadline else ""
                        row["sessions"] = sessions
                        row["auto_publish"] = bool(request.form.get("auto_publish"))
                        break
                api["save_weekends_data"](data)
            except ValueError as exc:
                flash("Horaires invalides : " + str(exc))
                return redirect(url_for("admin_schedule", weekend_id=weekend_id))
            flash("Horaires enregistrés. Le serveur applique automatiquement la clôture.")
            return redirect(url_for("admin_schedule", weekend_id=weekend_id))
        sessions_lines = "\n".join(
            s.get("label", "") + "|" + paris_input(s.get("utc"))
            for s in weekend.get("sessions", []) if isinstance(s, dict)
        )
        return render_template("admin_schedule.html", w=weekend,
                               csrf=csrf_value(), deadline=paris_input(weekend.get("deadline_utc")),
                               sessions_text=sessions_lines, name=api["current_player"](request)[0])

    @app.route("/avatar/<int:participant_id>")
    def player_avatar(participant_id):
        if not engine:
            abort(404)
        api["ensure_db_bootstrap"]()
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT photo_data FROM player_photos WHERE participant_id=:id"),
                {"id": participant_id}).fetchone()
        if not row:
            abort(404)
        response = send_file(io.BytesIO(bytes(row[0])), mimetype="image/jpeg", max_age=3600)
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    @app.route("/profile", methods=["GET", "POST"])
    def profile():
        person = connected()
        if not person:
            return redirect(url_for("login"))
        if request.method == "POST":
            check_csrf()
            if not engine:
                abort(503)
            if request.form.get("operation") == "delete":
                with engine.begin() as conn:
                    conn.execute(text("DELETE FROM player_photos WHERE participant_id=:id"),
                                 {"id": person["id"]})
                flash("Photo supprimée.")
            else:
                upload = request.files.get("photo")
                if not upload:
                    abort(400)
                # Lecture bornée (10 Mo maximum) ; ne pas faire confiance à l'extension du fichier.
                blob = upload.stream.read(MAX_PHOTO_SIZE + 1)
                if len(blob) > MAX_PHOTO_SIZE or not blob:
                    abort(413)
                try:
                    image = Image.open(io.BytesIO(blob))
                    image.verify()
                    image = Image.open(io.BytesIO(blob))
                    image = ImageOps.exif_transpose(image)
                    if image.width * image.height > 45_000_000:
                        abort(413)
                    image.thumbnail((512, 512), Image.Resampling.LANCZOS)
                    buf = io.BytesIO()
                    image.convert("RGB").save(buf, "JPEG", quality=82, optimize=True)
                except (UnidentifiedImageError, OSError, ValueError):
                    abort(400)
                with engine.begin() as conn:
                    conn.execute(text("""
                        INSERT INTO player_photos (participant_id, photo_data, updated_at)
                        VALUES (:id, :photo, NOW())
                        ON CONFLICT (participant_id)
                        DO UPDATE SET photo_data=EXCLUDED.photo_data, updated_at=NOW()
                    """), {"id": person["id"], "photo": buf.getvalue()})
                flash("Photo enregistrée.")
            return redirect(url_for("profile"))
        return render_template("profile.html", name=person["pseudo"], person=person,
                               csrf=csrf_value())

    @app.route("/me")
    def player_dashboard():
        person = connected()
        if not person:
            return redirect(url_for("login"))
        weekends = api["get_sorted_weekends"]()
        myname = api["normalize_pseudo"](person["pseudo"])
        all_totals = {}
        gp_count = 0
        last_result = None
        played = {}
        for w in weekends:
            if w.get("cancelled"):
                continue
            entries = api["get_latest_pronos_by_player_for_weekend"](w["id"])
            players = {api["normalize_pseudo"](k): (k,v) for k,v in entries.items()}
            if myname in players:
                played[w["id"]] = True
            result = api["load_results"](w["id"])
            if not result:
                continue
            review = api["load_bonus_reviews"](w["id"])
            for normalized, (player_name,prediction) in players.items():
                pts = api["compute_points_breakdown"](
                    prediction, result, w, player_name=player_name, reviews=review)["total"]
                all_totals[normalized] = round(all_totals.get(normalized,0)+pts, 2)
                if normalized == myname:
                    gp_count += 1
                    last_result = {"name": w.get("label"), "points": pts}
        standings = sorted(all_totals, key=lambda n:(-all_totals[n],n))
        rank = standings.index(myname)+1 if myname in standings else None
        for w in weekends:
            w["played"] = bool(played.get(w["id"]))
            w["closed"] = api["is_weekend_closed"](w["id"])
        return render_template("player_dashboard.html",
            name=person["pseudo"], person=person,
            weekends=weekends, points=all_totals.get(myname,0), rank=rank,
            gp_count=gp_count, last_result=last_result,
            next_gp=race_center(weekends))
