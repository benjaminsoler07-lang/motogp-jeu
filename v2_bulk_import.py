"""Import en bloc du programme MotoGP, exclusivement sous contrôle de l'admin.

Deux étapes indépendantes (aperçu puis confirmation) ; pas de stockage d'un
calendrier complet dans les cookies Flask. Un digest lié à la session protège
les correspondances et l'état de tous les GP entre les deux étapes.
"""
from __future__ import annotations

import hmac
import secrets
from datetime import datetime, timezone
from flask import request, session, redirect, url_for, flash, abort, render_template
from sqlalchemy import text

from motogp_bulk import (
    suggest_motogp_event, q1_datetime, batch_preview_digest, proposal_changes
)
from motogp_schedule import fetch_motogp_events, MotoGPAPIError
from v2_features import deadline_from_weekend, paris_input


def register_bulk_import(app, api):
    engine = api["engine"]

    def csrf_token():
        if not session.get("v2_csrf"):
            session["v2_csrf"] = secrets.token_urlsafe(32)
        return session["v2_csrf"]

    def require_csrf():
        expected = session.get("v2_csrf") or ""
        if not expected or not hmac.compare_digest(
            str(request.form.get("csrf") or ""), str(expected)
        ):
            abort(403)

    def build_rows(year, events, associations=None):
        now = datetime.now(timezone.utc)
        today = now.date()
        external = {item["external_id"]: item for item in events}

        manual_closed = {}
        if engine:
            with engine.connect() as conn:
                manual_closed = {
                    row[0]: bool(row[1])
                    for row in conn.execute(
                        text("SELECT weekend_id,closed_at FROM weekends")
                    ).fetchall()
                }
        weekends = [
            w for w in api["get_sorted_weekends"]()
            if api["weekend_season"](w) == year
        ]
        rows = []
        for w in weekends:
            wid = w["id"]
            if associations is None:
                choice = suggest_motogp_event(wid, events)
            else:
                chosen_id = str(associations.get(wid) or "")
                if chosen_id and chosen_id not in external:
                    raise ValueError("GP officiel non reconnu pour " + wid)
                choice = external.get(chosen_id)

            deadline = deadline_from_weekend(w)
            original_past = bool(deadline and deadline <= now)
            event_day_past = bool(choice and choice["date"] < today.isoformat())
            cancelled = bool(w.get("cancelled"))
            closed = (manual_closed.get(wid, False) or original_past
                      or event_day_past or cancelled)
            q1 = q1_datetime(choice) if choice else None
            rows.append({
                "id": wid,
                "label": w.get("label") or wid,
                "year": year,
                "current_date": w.get("date") or "",
                "current_deadline": w.get("deadline_utc") or "",
                "current_deadline_paris": paris_input(w.get("deadline_utc")),
                "deadline_source": w.get("deadline_source") or "",
                "current_session_count": len(w.get("sessions") or []),
                "closed": closed,
                "cancelled": cancelled,
                "event": choice,
                "q1": q1.isoformat() if q1 else "",
                "q1_paris": paris_input(q1.isoformat()) if q1 else "",
                "q1_future": bool(q1 and q1 > now),
                "revision": api["weekend_config_revision"](w),
            })
        return rows

    def check_mapping_uniqueness(rows):
        used = {}
        for r in rows:
            eid = (r["event"] or {}).get("external_id")
            if eid:
                if eid in used:
                    raise ValueError(
                        "La même épreuve MotoGP est associée à deux GP ("
                        + used[eid] + " et " + r["label"] + "). Corrige les associations."
                    )
                used[eid] = r["label"]

    @app.route("/admin/calendar/sync_motogp", methods=["GET", "POST"])
    def admin_bulk_motogp():
        if not api["is_admin"]():
            abort(403)
        if not engine:
            abort(503)

        raw_year = request.values.get("season") or str(api["selected_season_year"]())
        try:
            year = int(raw_year)
        except (ValueError, TypeError):
            abort(400)
        if not 2020 <= year <= 2100:
            abort(400)
        if api["is_season_archived"](year):
            flash("Cette saison est archivée : synchronisation désactivée.")
            return redirect(url_for("admin_calendar"))

        if request.method == "POST":
            require_csrf()
        action = request.form.get("action") if request.method == "POST" else "choose"
        if action not in ("choose", "preview", "apply"):
            abort(400)

        def page(rows, events, stage="choose", api_error="", report=None):
            return render_template(
                "admin_bulk_motogp.html", rows=rows, events=events,
                year=year, stage=stage, api_error=api_error,
                csrf=csrf_token(), name=api["current_player"](request)[0],
                report=report
            )

        try:
            # Les appels sont tous en lecture. Le serveur ne modifie jamais
            # ses paramètres avant la deuxième validation de l'administrateur.
            events = fetch_motogp_events(year)
            associations = None
            if action in ("preview", "apply"):
                ids = {
                    w["id"] for w in api["get_sorted_weekends"]()
                    if api["weekend_season"](w) == year
                }
                associations = {
                    wid: str(request.form.get("event_" + wid) or "")
                    for wid in ids
                }
            rows = build_rows(year, events, associations)
            if action in ("preview", "apply"):
                check_mapping_uniqueness(rows)
        except (MotoGPAPIError, ValueError) as exc:
            if action == "apply":
                session.pop("bulk_motogp_preview", None)
            return page([], [], api_error=str(exc))

        if action == "choose":
            return page(rows, events)

        digest = batch_preview_digest(year, rows)
        if action == "preview":
            if not any(r["event"] and not r["cancelled"] for r in rows):
                flash("Associe au moins un GP de PronoGP à une épreuve MotoGP.")
                return page(rows, events)
            session["bulk_motogp_preview"] = {
                "year": year, "digest": digest,
                "at": datetime.now(timezone.utc).timestamp()
            }
            return page(rows, events, stage="preview")

        token = session.get("bulk_motogp_preview") or {}
        age = datetime.now(timezone.utc).timestamp() - float(token.get("at") or 0)
        if (token.get("year") != year
                or not isinstance(token.get("digest"), str)
                or not hmac.compare_digest(token["digest"], digest)
                or not 0 <= age <= 900):
            session.pop("bulk_motogp_preview", None)
            flash("L'aperçu a expiré ou le calendrier a changé. Recommence l'import.")
            return redirect(url_for("admin_bulk_motogp", season=year))

        selected = request.form.getlist("selected_gp")
        known_ids = {r["id"] for r in rows}
        if not selected or len(selected) > 50 or len(set(selected)) != len(selected):
            flash("Sélectionne au moins un GP, sans doublon.")
            return page(rows, events, stage="preview")
        if any(wid not in known_ids for wid in selected):
            abort(400)
        change_dates = request.form.get("change_dates") == "1"
        schedule_q1 = request.form.get("schedule_q1") == "1"
        override_manual = request.form.get("override_manual") == "1"
        if request.form.get("final_confirmation") != "1":
            flash("Confirme explicitement l'import global avant d'enregistrer.")
            return page(rows, events, stage="preview")

        plans = []
        count_q1 = count_dates = count_past = count_kept_manual = count_missing_q1 = 0
        now = datetime.now(timezone.utc)
        try:
            for row in rows:
                if row["id"] not in selected:
                    continue
                if row["cancelled"] or not row["event"]:
                    raise ValueError(
                        "Le GP " + row["label"] +
                        " est annulé ou sans association officielle."
                    )
                updates, q1_result = proposal_changes(
                    row, now=now, change_dates=change_dates,
                    schedule_q1=schedule_q1,
                    override_manual=override_manual
                )
                if "deadline_utc" in updates:
                    count_q1 += 1
                if "date" in updates:
                    count_dates += 1
                if row["closed"]:
                    count_past += 1
                if q1_result == "Clôture personnalisée préservée":
                    count_kept_manual += 1
                if q1_result == "Q1 introuvable : clôture préservée":
                    count_missing_q1 += 1
                plans.append({
                    "id": row["id"], "updates": updates,
                    "revision": row["revision"]
                })
        except ValueError as error:
            flash("Import refusé : " + str(error))
            return page(rows, events, stage="preview")

        # Toute la sélection est appliquée en une SEULE transaction PostgreSQL.
        ok, message = api["update_weekend_configs_bulk"](plans)
        if not ok:
            session.pop("bulk_motogp_preview", None)
            flash("Aucun changement enregistré : " + message)
            return redirect(url_for("admin_bulk_motogp", season=year))
        session.pop("bulk_motogp_preview", None)
        flash(
            "%s · %s séances GP importées · %s dates futures actualisées · "
            "%s clôtures Q1 programmées · %s GP déjà clôturés conservés. "
            "%s clôtures manuelles préservées · %s GP sans Q1 exploitable. "
            "Publication automatique inchangée."
            % (message, len(plans), count_dates, count_q1, count_past,
               count_kept_manual, count_missing_q1)
        )
        return redirect(url_for("admin_bulk_motogp", season=year))
