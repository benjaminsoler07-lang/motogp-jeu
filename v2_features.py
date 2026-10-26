"""PronoGP V2: fonctions additives (profil, calendrier de séances, espace joueur).
Aucune modification destructive des tables de pronostics existantes.
"""
from __future__ import annotations

import io
import re
import secrets
import hmac
import json
from datetime import datetime, timezone, timedelta
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
    # Les heures absentes ou ambigües lors du changement d'heure sont interdites.
    if aware.astimezone(UTC).astimezone(PARIS).replace(tzinfo=None) != dt:
        raise ValueError("Horaire inexistant lors du changement d'heure.")
    alternate = dt.replace(tzinfo=PARIS, fold=1)
    if aware.utcoffset() != alternate.utcoffset():
        raise ValueError("Horaire ambigu lors du changement d'heure : choisir un autre horaire.")
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
        else:
            # GP anciens sans séances configurées : on conserve un Race Center,
            # sans inventer d'heure de session ou de clôture.
            date_text = str(weekend.get("date") or "")
            try:
                from datetime import date, time
                day = date.fromisoformat(date_text)
                event_day = datetime.combine(day, time(12, 0), tzinfo=PARIS).astimezone(UTC)
                if event_day >= now:
                    upcoming.append((event_day, weekend, None))
            except ValueError:
                pass
    if not upcoming:
        return None
    upcoming.sort(key=lambda value: value[0])
    _, weekend, session_next = upcoming[0]
    deadline = deadline_from_weekend(weekend)
    return {
        "id": weekend["id"],
        "name": weekend.get("label", ""),
        "public": bool(weekend.get("public")),
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

    @app.route("/admin/seasons", methods=["GET", "POST"])
    def admin_seasons():
        admin_only()
        if not engine:
            abort(503)
        api["ensure_db_bootstrap"]()
        if request.method == "POST":
            check_csrf()
            action = request.form.get("action")
            try:
                year = int(request.form.get("year") or "")
                if not (2026 <= year <= 2100):
                    raise ValueError("Année invalide.")
                if action == "add":
                    with engine.begin() as conn:
                        conn.execute(text("""
                            INSERT INTO seasons(year, archived) VALUES (:y, FALSE)
                            ON CONFLICT (year) DO NOTHING
                        """), {"y": year})
                        # Nouveau championnat : liste de départ modifiable, sans effet sur 2026.
                        if year != 2026:
                            conn.execute(text("""
                                INSERT INTO season_riders(season_year, rider, active)
                                SELECT :y, rider, active FROM season_riders
                                WHERE season_year=2026 AND removed=FALSE
                                ON CONFLICT(season_year,rider) DO NOTHING
                            """), {"y": year})
                            conn.execute(text("""
                                INSERT INTO season_teams(season_year,team,kind,active)
                                SELECT :y,team,kind,active FROM season_teams
                                WHERE season_year=2026 AND removed=FALSE
                                ON CONFLICT(season_year,team) DO NOTHING
                            """), {"y": year})
                    flash("Saison créée (liste de pilotes copiée, à vérifier).")
                elif action == "archive":
                    with engine.begin() as conn:
                        conn.execute(text("UPDATE seasons SET archived=:a WHERE year=:y"),
                                     {"a": request.form.get("archived") == "1", "y": year})
                    flash("Statut de la saison mis à jour.")
                elif action == "gp":
                    label = str(request.form.get("label") or "").strip()
                    date_raw = str(request.form.get("date") or "").strip()
                    if len(label) < 3 or len(label) > 120:
                        raise ValueError("Nom de GP invalide.")
                    parsed = datetime.strptime(date_raw, "%Y-%m-%d")
                    if parsed.year != year:
                        raise ValueError("La date doit appartenir à la saison sélectionnée.")
                    slug = re.sub(r"[^a-z0-9]+", "-", label.casefold().encode("ascii","ignore").decode()).strip("-")
                    wid = str(year) + "-" + slug
                    if not slug:
                        raise ValueError("Nom de GP invalide.")
                    with engine.begin() as conn:
                        season = conn.execute(text("SELECT archived FROM seasons WHERE year=:y"), {"y":year}).fetchone()
                    if season is None or season[0]:
                        raise ValueError("Créer ou rouvrir la saison avant d'ajouter ce GP.")
                    data = api["load_weekends_data"]()
                    if any(row.get("id")==wid for row in data["weekends"]):
                        raise ValueError("Ce GP existe déjà.")
                    data["weekends"].append({"id":wid,"label":label,
                        "date":date_raw,"season_year":year,"bonus_questions":[],
                        "cancelled":False})
                    api["save_weekends_data"](data)
                    flash("Grand Prix ajouté.")
                else:
                    abort(400)
            except (ValueError, TypeError) as exc:
                flash(str(exc))
            return redirect(url_for("admin_seasons"))
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT year, archived FROM seasons ORDER BY year DESC")).mappings().all()
        counts = {}
        for w in api["get_sorted_weekends"]():
            y = api["weekend_season"](w)
            counts[y] = counts.get(y,0)+1
        return render_template("admin_seasons.html", name=api["current_player"](request)[0],
                               seasons=rows, counts=counts, csrf=csrf_value())

    @app.route("/admin/riders", methods=["GET", "POST"])
    def admin_riders():
        admin_only()
        if not engine:
            abort(503)
        api["ensure_db_bootstrap"]()
        try:
            year = int(api["selected_season_year"]())
            if year < 2026 or year > 2100:
                abort(400)
        except ValueError:
            abort(400)
        with engine.connect() as conn:
            season=conn.execute(text("SELECT archived FROM seasons WHERE year=:year"),
                                {"year":year}).fetchone()
        if not season:
            flash("Saison inconnue : crée-la d'abord.")
            return redirect(url_for("admin_seasons"))
        if request.method == "POST":
            check_csrf()
            if season[0]:
                flash("Saison archivée : pilotes non modifiables.")
                return redirect(url_for("admin_riders", season=year))
            action=request.form.get("action") or ""
            rider=(request.form.get("rider") or "").strip()
            if action == "add":
                if len(rider)>100 or not re.fullmatch(r"#[0-9]{1,3} [A-Za-zÀ-ÿ].{1,90}", rider):
                    flash("Format requis : #49 Fabio Di Giannantonio")
                    return redirect(url_for("admin_riders", season=year))
                with engine.begin() as conn:
                    conn.execute(text("""
                        INSERT INTO season_riders(season_year,rider,active)
                        VALUES (:year,:rider,TRUE)
                        ON CONFLICT(season_year,rider)
                        DO UPDATE SET active=TRUE,removed=FALSE
                    """),{"year":year,"rider":rider})
                flash("Pilote ajouté / réactivé.")
            elif action == "toggle":
                with engine.begin() as conn:
                    changed=conn.execute(text("""
                        UPDATE season_riders SET active=NOT active
                        WHERE season_year=:year AND rider=:rider AND removed=FALSE
                    """),{"year":year,"rider":rider}).rowcount
                flash("Visibilité du pilote mise à jour." if changed else "Pilote introuvable.")
            elif action == "remove":
                # Seul le catalogue de cette saison est modifié. Les pronostics
                # déjà enregistrés / historiques ne sont jamais supprimés.
                with engine.begin() as conn:
                    changed = conn.execute(text("""
                        UPDATE season_riders SET removed=TRUE, active=FALSE
                        WHERE season_year=:year AND rider=:rider AND removed=FALSE
                    """), {"year":year,"rider":rider}).rowcount
                flash("Pilote retiré de cette saison (historique conservé)." if changed
                      else "Pilote introuvable.")
            else:
                abort(400)
            return redirect(url_for("admin_riders", season=year))
        with engine.connect() as conn:
            rows=conn.execute(text("""
                SELECT rider, active FROM season_riders
                WHERE season_year=:year AND removed=FALSE ORDER BY rider
            """),{"year":year}).mappings().all()
            seasons=conn.execute(text("SELECT year FROM seasons ORDER BY year DESC")).scalars().all()
        return render_template("admin_riders.html",name=api["current_player"](request)[0],
                               year=year,seasons=seasons,
                               active_rows=[r for r in rows if r["active"]],
                               hidden_rows=[r for r in rows if not r["active"]],
                               archived=bool(season[0]),csrf=csrf_value())

    @app.route("/admin/teams", methods=["GET", "POST"])
    def admin_teams():
        admin_only()
        if not engine:
            abort(503)
        api["ensure_db_bootstrap"]()
        year=int(api["selected_season_year"]())
        with engine.connect() as conn:
            season=conn.execute(text("SELECT archived FROM seasons WHERE year=:y"),{"y":year}).fetchone()
        if not season:
            flash("Créer la saison avant de gérer ses équipes.")
            return redirect(url_for("admin_seasons"))
        if request.method=="POST":
            check_csrf()
            if season[0]:
                abort(403)
            action=request.form.get("action") or ""
            team=(request.form.get("team") or "").strip()
            kind=request.form.get("kind") or ""
            if action=="add":
                if not (2<=len(team)<=100 and kind in ("official","satellite")):
                    abort(400)
                with engine.begin() as conn:
                    conn.execute(text("""
                        INSERT INTO season_teams(season_year,team,kind,active)
                        VALUES (:year,:team,:kind,TRUE)
                        ON CONFLICT(season_year,team)
                        DO UPDATE SET active=TRUE,kind=EXCLUDED.kind,removed=FALSE
                    """),{"year":year,"team":team,"kind":kind})
                flash("Équipe ajoutée / réactivée.")
            elif action=="toggle":
                with engine.begin() as conn:
                    changed=conn.execute(text("""
                        UPDATE season_teams SET active=NOT active
                        WHERE season_year=:year AND team=:team AND removed=FALSE
                    """),{"year":year,"team":team}).rowcount
                flash("Visibilité de l'écurie mise à jour." if changed else "Écurie introuvable.")
            elif action=="remove":
                # Pas de suppression de pronostics ou de résultats historiques.
                with engine.begin() as conn:
                    changed=conn.execute(text("""
                        UPDATE season_teams SET removed=TRUE, active=FALSE
                        WHERE season_year=:year AND team=:team AND removed=FALSE
                    """),{"year":year,"team":team}).rowcount
                flash("Écurie retirée de cette saison (historique conservé)." if changed
                      else "Écurie introuvable.")
            else:
                abort(400)
            return redirect(url_for("admin_teams",season=year))
        with engine.connect() as conn:
            rows=conn.execute(text("""
                SELECT team,kind,active FROM season_teams
                WHERE season_year=:year AND removed=FALSE ORDER BY kind,team
            """),{"year":year}).mappings().all()
            seasons=conn.execute(text("SELECT year FROM seasons ORDER BY year DESC")).scalars().all()
        return render_template("admin_teams.html",name=api["current_player"](request)[0],
                               year=year,
                               active_rows=[r for r in rows if r["active"]],
                               hidden_rows=[r for r in rows if not r["active"]],
                               archived=bool(season[0]),seasons=seasons,
                               csrf=csrf_value())

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
                # Modification MANUELLE explicite : l'admin peut corriger
                # une heure erronée, même si l'ancienne clôture est passée.
                # L'import API garde, lui, une protection renforcée.
                ok, message = api["update_weekend_config_fields"](weekend_id, {
                    "deadline_utc": deadline.isoformat() if deadline else "",
                    "deadline_source": "manual",
                    "sessions": sessions,
                    "auto_publish": bool(request.form.get("auto_publish"))
                })
                if not ok:
                    raise ValueError(message)
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


    @app.route("/admin/w/<weekend_id>/sync_motogp", methods=["GET", "POST"])
    def admin_sync_motogp(weekend_id):
        """Import volontaire : l'API ne peut modifier ni fermer un GP seule."""
        from motogp_schedule import fetch_motogp_events, MotoGPAPIError
        admin_only()
        weekend = api["get_weekend"](weekend_id)
        if not weekend:
            abort(404)
        year = int(api["weekend_season"](weekend))
        if api["is_season_archived"](year):
            abort(403)

        preview = session.get("motogp_sync_preview")
        if not isinstance(preview, dict) or preview.get("weekend_id") != weekend_id:
            preview = None
        else:
            # Une proposition ne peut être confirmée que pendant 20 min.
            age = datetime.now(UTC).timestamp() - float(preview.get("at") or 0)
            if age < 0 or age > 1200:
                session.pop("motogp_sync_preview", None)
                preview = None

        if request.method == "POST":
            check_csrf()
            action = request.form.get("action")
            if action == "preview":
                selected = str(request.form.get("external_id") or "").strip()
                try:
                    events = fetch_motogp_events(year)
                    chosen = next((item for item in events
                                   if item["external_id"] == selected), None)
                    if chosen is None:
                        raise ValueError("Sélectionne un Grand Prix de la liste.")
                    preview = {
                        "weekend_id": weekend_id,
                        "event": chosen,
                        "revision": api["weekend_config_revision"](weekend),
                        "at": datetime.now(UTC).timestamp(),
                    }
                    session["motogp_sync_preview"] = preview
                    flash("Aperçu préparé. Rien n'a encore été enregistré.")
                except (MotoGPAPIError, ValueError) as error:
                    flash(str(error))
                return redirect(url_for("admin_sync_motogp", weekend_id=weekend_id))

            if action == "apply":
                if preview is None:
                    flash("L'aperçu a expiré. Charge-le de nouveau.")
                    return redirect(url_for("admin_sync_motogp", weekend_id=weekend_id))
                proposed = preview["event"]
                changes = {}
                try:
                    if str(request.form.get("apply_date") or "") == "1":
                        day = datetime.fromisoformat(proposed["date"]).date()
                        if day.year != year:
                            raise ValueError("Date importée hors de la saison du GP.")
                        changes["date"] = day.isoformat()
                    if str(request.form.get("apply_sessions") or "") == "1":
                        if not proposed.get("sessions"):
                            raise ValueError("Aucune séance exploitable dans l'API.")
                        changes["sessions"] = proposed["sessions"]

                    mode = request.form.get("deadline_mode") or "keep"
                    if mode == "session":
                        if request.form.get("confirm_deadline") != "1":
                            raise ValueError(
                                "Coche la confirmation explicite de la nouvelle clôture."
                            )
                        try:
                            choice = int(request.form.get("session_index") or "-1")
                            offset = int(request.form.get("minutes_before") or "0")
                        except ValueError as error:
                            raise ValueError("Heure de clôture invalide.") from error
                        if not 0 <= choice < len(proposed["sessions"]) or not 0 <= offset <= 120:
                            raise ValueError("Séance ou avance de clôture invalide.")
                        selected_session = proposed["sessions"][choice]
                        when = datetime.fromisoformat(selected_session["utc"])
                        new_deadline = when - timedelta(minutes=offset)
                        old_deadline = deadline_from_weekend(weekend)
                        now = datetime.now(UTC)
                        if api["is_weekend_closed"](weekend_id):
                            raise ValueError(
                                "Le GP est déjà clôturé : aucune nouvelle heure de "
                                "clôture ne peut être appliquée par import."
                            )
                        if new_deadline <= now and request.form.get("confirm_past") != "1":
                            raise ValueError(
                                "Cette heure est déjà passée et fermerait les pronostics. "
                                "Confirme explicitement une clôture passée."
                            )
                        if old_deadline and old_deadline <= now and new_deadline > now:
                            raise ValueError(
                                "Refus de rouvrir une clôture passée via l'API."
                            )
                        changes["deadline_utc"] = new_deadline.isoformat()
                        changes["deadline_source"] = (
                            "motogp_q1" if selected_session["label"] == "Q1"
                            and offset == 0 else "manual"
                        )
                    elif mode != "keep":
                        raise ValueError("Mode de clôture inconnu.")

                    if not changes:
                        raise ValueError("Coche au moins une donnée à importer.")
                    ok, message = api["update_weekend_config_fields"](
                        weekend_id, changes, expected_revision=preview["revision"]
                    )
                    if not ok:
                        raise ValueError(message)
                    session.pop("motogp_sync_preview", None)
                    flash("Import validé : " + ", ".join(changes) +
                          ". Clôture et publication manuelles conservées sauf choix explicite.")
                    return redirect(url_for("admin_schedule", weekend_id=weekend_id))
                except (ValueError, KeyError, IndexError) as error:
                    flash("Aucun changement appliqué : " + str(error))
                    return redirect(url_for("admin_sync_motogp", weekend_id=weekend_id))
            abort(400)

        events = []
        api_error = ""
        try:
            events = fetch_motogp_events(year)
        except MotoGPAPIError as exc:
            api_error = str(exc)
        return render_template(
            "admin_motogp_sync.html",
            w=weekend, year=year, events=events, preview=preview,
            api_error=api_error, csrf=csrf_value(),
            deadline=paris_input(weekend.get("deadline_utc")),
            name=api["current_player"](request)[0], to_paris=paris_input
        )

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

    def profile_summary(person):
        """Résumé de saison présenté directement sur Mon profil (ancienne page /me)."""
        year = api["selected_season_year"]()
        weekends = [w for w in api["get_sorted_weekends"]()
                    if api["weekend_season"](w) == year]
        myname = api["normalize_pseudo"](person["pseudo"])
        all_totals = {}
        gp_count = 0
        last_result = None
        for w in weekends:
            if w.get("cancelled"):
                continue
            entries = api["get_latest_pronos_by_player_for_weekend"](w["id"])
            players = {api["normalize_pseudo"](k): (k,v) for k,v in entries.items()}
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
        return {
            "points": all_totals.get(myname,0),
            "rank": rank,
            "gp_count": gp_count,
            "last_result": last_result,
            "season_year": year
        }

    @app.route("/me")
    def player_dashboard():
        # Conserver les anciens liens/favoris vers Mon espace, sans doublon de page.
        year = request.args.get("season")
        return redirect(url_for("profile", season=year)) if year else redirect(url_for("profile"))

    @app.route("/profile", methods=["GET", "POST"])
    def profile():
        person = connected()
        if not person:
            return redirect(url_for("login"))
        if request.method == "POST":
            check_csrf()
            operation = request.form.get("operation") or "upload_photo"
            if operation == "change_password":
                old_password = request.form.get("current_password") or ""
                new_password = request.form.get("new_password") or ""
                confirmation = request.form.get("confirm_password") or ""
                if new_password != confirmation:
                    flash("Les deux nouveaux mots de passe ne correspondent pas.")
                else:
                    ok, message = api["change_participant_secret"](
                        person, old_password, new_password
                    )
                    if ok:
                        # Conserver CETTE session; toutes les anciennes sessions,
                        # sur les autres appareils, seront invalidées.
                        refreshed = api["get_participant_by_input"](person["pseudo"])
                        session["authenticated_secret_marker"] = api[
                            "participant_secret_marker"
                        ](refreshed)
                        session["v2_csrf"] = secrets.token_urlsafe(32)
                    flash(message)
                return redirect(url_for("profile"))
            if operation not in ("delete", "upload_photo"):
                abort(400)
            if not engine:
                abort(503)
            if operation == "delete":
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
                               csrf=csrf_value(), **profile_summary(person))

