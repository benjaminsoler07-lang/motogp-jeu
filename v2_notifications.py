"""Notifications PronoGP — boîte de messages, abonnements Web Push, rappels GitHub Actions.
Les messages sont indépendants de la réception effective d'un push sur le téléphone.
"""
from __future__ import annotations

import json
import logging
import os
import re
import secrets
import hmac
from datetime import datetime, timezone, timedelta
from urllib.parse import urlsplit

from flask import request, session, jsonify, abort, url_for, render_template, redirect, flash, send_from_directory
from sqlalchemy import text
from pywebpush import webpush, WebPushException
import jwt
from jwt import PyJWKClient

LOG = logging.getLogger(__name__)
ISSUER = "https://token.actions.githubusercontent.com"
AUDIENCE = "motogp-push-dispatch"
REPOSITORY = "benjaminsoler07-lang/motogp-jeu"
OWNER_ID = "260292963"
REPOSITORY_ID = "1153059915"
WORKFLOW = REPOSITORY + "/.github/workflows/push-hourly.yml@refs/heads/main"
JWKS = PyJWKClient(ISSUER + "/.well-known/jwks", lifespan=3600)
HOSTS = ("googleapis.com", "mozilla.com", "push.apple.com", "windows.com", "microsoft.com")
CATEGORIES = ("manual", "deadlines", "results", "schedule")


def valid_endpoint(url):
    try:
        p=urlsplit(url)
        domain=(p.hostname or "").lower()
        return (isinstance(url,str) and len(url)<2048 and p.scheme=="https"
                and p.port in (None,443) and not p.username and not p.password
                and not p.fragment and any(domain==h or domain.endswith("."+h) for h in HOSTS))
    except (TypeError,ValueError):
        return False


def valid_github_token(header):
    if not header.startswith("Bearer ") or len(header)>12000:
        return False
    token=header[7:]
    if token.count(".")!=2:
        return False
    try:
        key=JWKS.get_signing_key_from_jwt(token)
        claims=jwt.decode(token,key.key,algorithms=["RS256"],audience=AUDIENCE,issuer=ISSUER,
                          options={"require":["iss","aud","exp","iat","repository","ref"]},leeway=30)
    except Exception:
        LOG.warning("Invalid GitHub OIDC dispatch authorization")
        return False
    return (claims.get("repository")==REPOSITORY
            and str(claims.get("repository_id"))==REPOSITORY_ID
            and str(claims.get("repository_owner_id"))==OWNER_ID
            and claims.get("workflow_ref")==WORKFLOW
            and claims.get("ref")=="refs/heads/main"
            and claims.get("event_name") in ("schedule","workflow_dispatch"))


def register_notifications(app, api):
    engine=api["engine"]

    app.config["VAPID_PUBLIC_KEY"]=os.getenv("VAPID_PUBLIC_KEY","")
    app.config["VAPID_PRIVATE_KEY"]=os.getenv("VAPID_PRIVATE_KEY","").replace("\\n","\n")
    app.config["VAPID_SUBJECT"]=os.getenv("VAPID_SUBJECT","")

    def vapid_ready():
        return all(app.config.get(k) for k in ("VAPID_PUBLIC_KEY","VAPID_PRIVATE_KEY","VAPID_SUBJECT"))

    def person():
        p=api["get_current_participant"](request)
        if not p:
            abort(401)
        return p

    def csrf():
        if not session.get("v2_csrf"):
            session["v2_csrf"]=secrets.token_urlsafe(32)
        return session["v2_csrf"]

    def check_csrf():
        if not hmac.compare_digest(str(request.form.get("csrf") or ""),str(csrf())):
            abort(403)

    def enqueue(conn, *, users, category, title, body, destination, dedupe):
        if category not in CATEGORIES or not destination.startswith("/") or destination.startswith("//"):
            raise ValueError("bad notification")
        for user_id in set(map(int,users)):
            conn.execute(text("""
                INSERT INTO push_messages
                (participant_id, category, title, body, destination, dedupe_key)
                VALUES (:pid, :cat, :title, :body, :dest, :dedupe)
                ON CONFLICT (dedupe_key) DO NOTHING
            """),{"pid":user_id,"cat":category,"title":title[:100],
                  "body":body[:500],"dest":destination[:300],"dedupe":dedupe+":"+str(user_id)})

    @app.get("/manifest.webmanifest")
    def pwa_manifest():
        return jsonify({"name":"PronoGP","short_name":"PronoGP",
                        "start_url":"/","display":"standalone",
                        "theme_color":"#0d3b66","background_color":"#0d3b66",
                        "icons":[{"src":"/pwa/icon/192","sizes":"192x192","type":"image/png"},
                                 {"src":"/pwa/icon/512","sizes":"512x512","type":"image/png"}]}),200,{"Content-Type":"application/manifest+json"}

    @app.get("/pwa/icon/<int:size>")
    def pwa_icon(size):
        if size not in (192,512):
            abort(404)
        from PIL import Image, ImageDraw
        from io import BytesIO
        art=Image.new("RGB",(size,size),"#0d3b66")
        draw=ImageDraw.Draw(art)
        radius=size//6
        draw.rounded_rectangle((0,0,size-1,size-1),radius=radius,fill="#0d3b66")
        draw.ellipse((size*.2,size*.2,size*.8,size*.8),fill="#ffd60a")
        draw.ellipse((size*.34,size*.34,size*.66,size*.66),fill="#0d3b66")
        draw.line((size*.2,size*.78,size*.8,size*.78),fill="#fff",width=max(4,size//25))
        b=BytesIO();art.save(b,"PNG");b.seek(0)
        from flask import send_file
        return send_file(b,mimetype="image/png",max_age=86400)

    @app.get("/service-worker.js")
    def pwa_service_worker():
        response=send_from_directory(app.static_folder,"service-worker.js",mimetype="text/javascript")
        response.headers["Service-Worker-Allowed"]="/"
        return response

    @app.get("/notifications")
    def notifications():
        p=person()
        if not engine:
            abort(503)
        with engine.connect() as conn:
            messages=conn.execute(text("""
                SELECT id, title, body, destination, category, created_at, read_at
                FROM push_messages WHERE participant_id=:pid ORDER BY id DESC LIMIT 200
            """),{"pid":p["id"]}).mappings().all()
        return render_template("notifications.html", name=p["pseudo"], messages=messages,
                               csrf=csrf(), public_key=app.config["VAPID_PUBLIC_KEY"],
                               push_configured=vapid_ready())

    @app.post("/notifications/<int:message_id>/read")
    def notification_read(message_id):
        p=person();check_csrf()
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE push_messages SET read_at=NOW()
                WHERE id=:mid AND participant_id=:pid AND read_at IS NULL
            """),{"mid":message_id,"pid":p["id"]})
        return redirect(url_for("notifications"))

    @app.post("/push/subscribe")
    def push_subscribe():
        p=person();check_csrf()
        if not vapid_ready():
            return jsonify(error="Web Push indisponible tant que les clés VAPID ne sont pas configurées."),503
        raw=request.form.get("subscription") or ""
        if len(raw)>8192:
            abort(400)
        try:
            data=json.loads(raw)
            endpoint=data["endpoint"]
            keys=data["keys"]
            p256dh,auth=keys["p256dh"],keys["auth"]
            if not valid_endpoint(endpoint) or not all(
                isinstance(x,str) and re.fullmatch(r"[A-Za-z0-9_-]{16,256}",x)
                for x in (p256dh,auth)): raise ValueError
        except (TypeError,KeyError,ValueError):
            abort(400)
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO web_push_subscriptions(endpoint,participant_id,p256dh,auth,updated_at)
                VALUES (:endpoint,:pid,:p256dh,:auth,NOW())
                ON CONFLICT(endpoint) DO UPDATE
                SET participant_id=EXCLUDED.participant_id,p256dh=EXCLUDED.p256dh,
                    auth=EXCLUDED.auth,updated_at=NOW()
            """),{"endpoint":endpoint,"pid":p["id"],"p256dh":p256dh,"auth":auth})
        return jsonify(ok=True)

    @app.post("/push/unsubscribe")
    def push_unsubscribe():
        p=person();check_csrf()
        endpoint=request.form.get("endpoint") or ""
        if len(endpoint)>2048:
            abort(400)
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM web_push_subscriptions WHERE endpoint=:e AND participant_id=:p"),
                         {"e":endpoint,"p":p["id"]})
        return jsonify(ok=True)

    @app.route("/admin/notifications",methods=["GET","POST"])
    def admin_notifications():
        if not api["is_admin"]():
            abort(403)
        if not engine:
            abort(503)
        players=api["get_all_participants"](include_inactive=False)
        gps=api["get_sorted_weekends"]()
        if request.method=="POST":
            check_csrf()
            title=(request.form.get("title") or "").strip()
            body=(request.form.get("body") or "").strip()
            if not (1<=len(title)<=100 and 1<=len(body)<=500):
                abort(400)
            target=request.form.get("target") or "notifications"
            pages={"home":url_for("home"),"my":url_for("player_dashboard"),
                   "ranking":url_for("classement_general"),
                   "results":url_for("results_by_race"),
                   "notifications":url_for("notifications")}
            if target in pages:
                dest=pages[target]
            elif target=="gp":
                wid=request.form.get("gp") or ""
                if not api["get_weekend"](wid):
                    abort(400)
                dest=url_for("pronos",weekend_id=wid)
            else:
                abort(400)
            destination=request.form.get("recipients") or "all"
            if destination=="all":
                ids=[p["id"] for p in players]
            elif destination=="selected":
                wanted=request.form.getlist("player_ids")
                if not wanted or len(wanted)>300 or any(not x.isdigit() for x in wanted):
                    abort(400)
                valid_ids={p["id"] for p in players}
                ids={int(x) for x in wanted}
                if not ids.issubset(valid_ids):
                    abort(400)
            else:
                abort(400)
            if not ids:
                flash("Aucun destinataire actif.")
                return redirect(url_for("admin_notifications"))
            with engine.begin() as conn:
                enqueue(conn,users=ids,category="manual",title=title,body=body,
                        destination=dest,dedupe="admin:"+secrets.token_hex(18))
            flash(f"Message ajouté à la boîte de {len(ids)} joueur(s). "
                  "Le push sera envoyé lors du prochain passage planifié.")
            return redirect(url_for("admin_notifications"))
        with engine.connect() as conn:
            history=conn.execute(text("""
                SELECT title, body, category, destination, created_at,
                       COUNT(*) AS recipients, COUNT(sent_at) AS processed
                FROM push_messages
                GROUP BY title, body, category, destination, created_at
                ORDER BY created_at DESC LIMIT 40
            """)).mappings().all()
        return render_template("admin_notifications.html",name=api["current_player"](request)[0],
                               players=players,gps=gps,csrf=csrf(),push_configured=vapid_ready(),
                               history=history)

    def reminders(conn):
        now=datetime.now(timezone.utc)
        # Une saisie/correction officielle produit un événement à destination des joueurs.
        # Les classements ne sont annoncés qu'après clôture, jamais avant la course.
        recent_results=conn.execute(text("""
            SELECT weekend_id, updated_at FROM results
            WHERE updated_at >= NOW() - INTERVAL '72 hours'
            ORDER BY updated_at DESC LIMIT 60
        """)).mappings().all()
        active_users=[p["id"] for p in api["get_all_participants"](include_inactive=False)]
        for result in recent_results:
            wid=result["weekend_id"]
            w=api["get_weekend"](wid)
            if not w or w.get("cancelled") or not api["is_weekend_closed"](wid):
                continue
            if active_users:
                enqueue(conn, users=active_users, category="results",
                        title="🏁 Résultats MotoGP : "+str(w.get("label",wid)),
                        body="Les résultats et le classement du Grand Prix sont disponibles.",
                        destination=url_for("results_by_race",gp=wid),
                        dedupe="results:"+wid+":"+result["updated_at"].isoformat())

        for w in api["get_sorted_weekends"]():
            if w.get("cancelled") or api["is_season_archived"](api["weekend_season"](w)):
                continue
            deadline=api["deadline_from_weekend"](w)
            if not deadline or deadline<=now:
                continue
            hours=(deadline-now).total_seconds()/3600
            category_label= "24h" if 20<=hours<=24 else ("2h" if 0<hours<=2 else None)
            if not category_label:
                continue
            participants=api["get_all_participants"](include_inactive=False)
            rows=conn.execute(text("""
                SELECT COALESCE(player_norm, LOWER(BTRIM(player_name))) AS player
                FROM pronos WHERE weekend_id=:w
            """),{"w":w["id"]}).fetchall()
            already={str(r[0]) for r in rows}
            users=[p["id"] for p in participants
                   if api["normalize_pseudo"](p["pseudo"]) not in already]
            if users:
                enqueue(conn,users=users,category="deadlines",
                        title="⏰ Prono MotoGP à remplir : "+str(w["label"]),
                        body=f"Tes pronostics ferment dans moins de {category_label}.",
                        destination=url_for("pronos",weekend_id=w["id"]),
                        dedupe=f"deadline:{w['id']}:{deadline.isoformat()}:{category_label}")

    def dispatch(conn):
        if not vapid_ready():
            return 0
        rows=conn.execute(text("""
            SELECT m.id,m.participant_id,m.title,m.body,m.destination,m.created_at,
                s.endpoint,s.p256dh,s.auth
            FROM push_messages m LEFT JOIN web_push_subscriptions s ON s.participant_id=m.participant_id
            WHERE m.sent_at IS NULL ORDER BY m.id ASC LIMIT 200
        """)).mappings().all()
        grouped={}
        for m in rows:
            grouped.setdefault(m["id"],[]).append(m)
        sent=0
        for msgrows in grouped.values():
            m=msgrows[0]
            failed=False
            old=(datetime.now(timezone.utc)-m["created_at"]).total_seconds()>86400
            for sub in msgrows:
                if not sub["endpoint"] or old:
                    continue
                try:
                    webpush(
                        subscription_info={"endpoint":sub["endpoint"],
                                          "keys":{"p256dh":sub["p256dh"],"auth":sub["auth"]}},
                        data=json.dumps({"title":m["title"],"body":m["body"],"url":m["destination"]},
                                        ensure_ascii=False),
                        vapid_private_key=app.config["VAPID_PRIVATE_KEY"],
                        vapid_claims={"sub":app.config["VAPID_SUBJECT"]},
                        ttl=3600,timeout=8)
                except WebPushException as exc:
                    if exc.status_code in (404,410):
                        conn.execute(text("DELETE FROM web_push_subscriptions WHERE endpoint=:e"),
                                     {"e":sub["endpoint"]})
                    else:
                        failed=True
                        LOG.warning("Web push failed with status %s",exc.status_code)
                except Exception:
                    failed=True
                    LOG.exception("Web Push delivery failed")
            if not failed:
                conn.execute(text("UPDATE push_messages SET sent_at=NOW() WHERE id=:id"),{"id":m["id"]})
                sent+=1
        return sent

    @app.post("/internal/github-dispatch")
    def github_dispatch():
        if not valid_github_token(request.headers.get("Authorization","")):
            abort(403)
        if not engine:
            abort(503)
        with engine.begin() as conn:
            reminders(conn)
            sent=dispatch(conn)
        return jsonify(ok=True,messages_processed=sent)
