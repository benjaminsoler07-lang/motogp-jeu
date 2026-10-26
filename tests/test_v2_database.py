import io
import os
import unittest
from PIL import Image
from sqlalchemy import text

if not os.getenv("DATABASE_URL"):
    raise unittest.SkipTest("Integration PostgreSQL unavailable")

from app import (
    app, engine, ensure_db_bootstrap, add_participant,
    get_participant_by_input, get_championnat_state, get_sorted_weekends
)


class DatabaseV2Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ensure_db_bootstrap()
        ok,msg=add_participant("IntegrationTest", "secure-test-123")
        if not ok and "existe déjà" not in msg:
            raise AssertionError(msg)

    def setUp(self):
        self.client=app.test_client()
        login=self.client.post("/login",data={"name":"IntegrationTest","secret":"secure-test-123"})
        self.assertEqual(login.status_code,302)
        with self.client.session_transaction() as st:
            self.csrf=st.setdefault("v2_csrf","integration-check")

    def test_player_identity_cannot_be_forged_with_cookie(self):
        other=app.test_client()
        other.set_cookie("player_name","IntegrationTest")
        response=other.get("/profile")
        self.assertEqual(response.status_code,302)
        self.assertEqual(self.client.get("/profile").status_code,200)

    def test_anonymous_visitors_cannot_browse_game(self):
        from urllib.parse import urlsplit

        visitor = app.test_client()
        player_id = get_participant_by_input("IntegrationTest")["id"]
        private_pages = (
            "/", "/classement", "/results_by_race", "/championnat/public",
            "/w/any-gp/public/pronos", "/w/any-gp/classement",
            "/me", "/profile", "/notifications", f"/avatar/{player_id}",
            "/admin", "/admin/seasons"
        )
        for path in private_pages:
            with self.subTest(path=path):
                response = visitor.get(path)
                self.assertEqual(response.status_code, 302)
                self.assertEqual(urlsplit(response.headers["Location"]).path, "/login")

        # Un cookie pseudo historique n'est pas une session authentifiee.
        visitor.set_cookie("player_name", "IntegrationTest")
        self.assertEqual(visitor.get("/classement").status_code, 302)

        # Les formulaires protegés ne sont pas accessibles en POST anonyme.
        self.assertEqual(visitor.post("/w/any-gp/pronos").status_code, 401)

        # Pages publiques nécessaires à la connexion et aux fonctions techniques.
        for path in ("/login", "/admin/login", "/healthz", "/manifest.webmanifest",
                     "/pwa/icon/192", "/service-worker.js"):
            with self.subTest(public=path):
                self.assertEqual(visitor.get(path).status_code, 200)

        # Le endpoint GitHub reste accessible au job OIDC, mais refuse les inconnus.
        self.assertEqual(visitor.post("/internal/github-dispatch").status_code, 403)

        # Un vrai joueur conserve l'accès aux pages de consultation.
        self.assertEqual(self.client.get("/").status_code, 200)
        self.assertEqual(self.client.get("/classement").status_code, 200)

    def test_admin_can_access_home_without_player_session(self):
        admin_client = app.test_client()
        with admin_client.session_transaction() as st:
            st["is_admin"] = True
        self.assertEqual(admin_client.get("/").status_code, 200)
        self.assertEqual(admin_client.get("/admin").status_code, 200)

    def test_homepage_is_focused_on_race_and_future_events(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("Le prochain Grand Prix", html)
        self.assertIn("v2-home-shortcuts", html)
        self.assertIn("Classement général", html)
        self.assertIn("Résultats par GP", html)
        self.assertIn("Prono championnat", html)
        self.assertNotIn("Une saison. Des Grands Prix", html)
        self.assertNotIn("Calendrier MotoGP", html)
        self.assertNotIn("v2-intro", html)
        self.assertLessEqual(html.count('class="v2-home-upcoming-card"'), 3)
        self.assertEqual(self.client.get("/results_by_race").status_code, 200)

    def test_admin_schedule_saves_only_one_gp_and_api_import_requires_confirmation(self):
        import json
        from datetime import datetime, timedelta
        from zoneinfo import ZoneInfo
        from unittest.mock import patch
        from app import get_weekend, weekend_config_revision
        from motogp_schedule import MotoGPAPIError

        wid = "test-motogp-sync-ci"
        other_id = "test-motogp-other-ci"
        initial = {
            "id": wid, "label": "GP de Malaisie", "date": "2026-11-30",
            "season_year": 2026, "sessions": [], "deadline_utc": "",
            "auto_publish": False, "cancelled": False,
        }
        another = {
            "id": other_id, "label": "GP non concerné", "date": "2026-12-01",
            "season_year": 2026, "sessions": [], "deadline_utc": "",
        }
        with engine.begin() as connection:
            for item in (initial, another):
                connection.execute(text("""
                    INSERT INTO weekends (weekend_id,config_json,updated_at)
                    VALUES (:id,CAST(:json AS jsonb),NOW())
                    ON CONFLICT (weekend_id)
                    DO UPDATE SET config_json=EXCLUDED.config_json,updated_at=NOW()
                """), {"id":item["id"],"json":json.dumps(item)})

        admin = app.test_client()
        with admin.session_transaction() as st:
            st["is_admin"] = True
        self.assertEqual(admin.get(f"/admin/w/{wid}/schedule").status_code, 200)
        with admin.session_transaction() as st:
            csrf = st["v2_csrf"]

        tomorrow = datetime.now(ZoneInfo("Europe/Paris")) + timedelta(days=20)
        manual_deadline = tomorrow.strftime("%Y-%m-%dT10:30")
        with patch("app.save_json", side_effect=OSError("Render read-only filesystem")):
            response = admin.post(f"/admin/w/{wid}/schedule", data={
                "csrf": csrf, "deadline": manual_deadline,
                "sessions": "Q1|" + manual_deadline, "auto_publish": "1"
            })
        self.assertEqual(response.status_code, 302)
        saved = get_weekend(wid)
        self.assertEqual(len(saved["sessions"]), 1)
        self.assertEqual(saved["sessions"][0]["label"], "Q1")
        self.assertTrue(saved["auto_publish"])
        self.assertEqual(get_weekend(other_id)["date"], "2026-12-01")
        saved_deadline = saved["deadline_utc"]

        from motogp_schedule import normalise_event
        candidate = normalise_event({
            "id": "api-malaysia-test", "name": "Malaysian Grand Prix",
            "kind": "GP", "season": {"year": 2026},
            "broadcasts": [
                {"shortname": "Q1", "type": "SESSION",
                 "category": {"name": "MotoGP", "acronym": "MGP"},
                 "date_start": "2026-10-31T10:50:00+0800"},
                {"shortname": "RAC", "type": "SESSION",
                 "category": {"name": "MotoGP", "acronym": "MGP"},
                 "date_start": "2026-11-01T15:00:00+0800"},
            ]
        },2026)
        self.assertEqual(candidate["date"], "2026-11-01")
        endpoint = f"/admin/w/{wid}/sync_motogp"

        visitor = app.test_client()
        self.assertEqual(visitor.get(endpoint).status_code, 302)
        self.assertEqual(admin.post(endpoint,data={
            "action":"preview","external_id":candidate["external_id"]
        }).status_code,403)

        with patch("motogp_schedule.fetch_motogp_events",return_value=[candidate]):
            self.assertEqual(admin.get(endpoint).status_code,200)
            preview_resp = admin.post(endpoint,data={
                "csrf":csrf,"action":"preview","external_id":candidate["external_id"]
            })
        self.assertEqual(preview_resp.status_code,302)
        with admin.session_transaction() as st:
            self.assertEqual(st["motogp_sync_preview"]["event"]["date"],"2026-11-01")
        self.assertEqual(get_weekend(wid)["date"],"2026-11-30")  # preview read-only
        with patch("motogp_schedule.fetch_motogp_events",return_value=[candidate]):
            overview = admin.get(endpoint)
        self.assertEqual(overview.status_code,200)
        overview_text = overview.get_data(as_text=True)
        self.assertIn("Comparer les informations",overview_text)
        self.assertIn("Q1",overview_text)
        self.assertIn("Conserver l'heure actuelle",overview_text)

        response=admin.post(endpoint,data={
            "csrf":csrf,"action":"apply", "apply_date":"1",
            "apply_sessions":"1","deadline_mode":"keep"
        })
        self.assertEqual(response.status_code,302)
        updated=get_weekend(wid)
        self.assertEqual(updated["date"],"2026-11-01")
        self.assertEqual(len(updated["sessions"]),2)
        self.assertEqual(updated["deadline_utc"],saved_deadline)
        self.assertEqual(updated["auto_publish"],True)
        self.assertEqual(get_weekend(other_id)["date"],"2026-12-01")

        # L'API en panne ne bloque pas la saisie manuelle et n'écrase rien.
        with patch("motogp_schedule.fetch_motogp_events",
                   side_effect=MotoGPAPIError("HTTP 503")):
            offline=admin.get(endpoint)
        self.assertEqual(offline.status_code,200)
        self.assertIn("HTTP 503",offline.get_data(as_text=True))
        self.assertEqual(get_weekend(wid)["date"],"2026-11-01")

        # Une modification entre aperçu et validation invalide la confirmation.
        with patch("motogp_schedule.fetch_motogp_events",return_value=[candidate]):
            self.assertEqual(admin.post(endpoint,data={
                "csrf":csrf,"action":"preview","external_id":candidate["external_id"]
            }).status_code,302)
        from app import update_weekend_config_fields
        ok, _=update_weekend_config_fields(wid,{"label":"GP malaisien modifié"})
        self.assertTrue(ok)
        blocked=admin.post(endpoint,data={
            "csrf":csrf,"action":"apply","apply_date":"1","deadline_mode":"keep"
        })
        self.assertEqual(blocked.status_code,302)
        self.assertEqual(get_weekend(wid)["label"],"GP malaisien modifié")

    def test_bulk_import_requires_admin_preview_and_schedules_q1_atomically(self):
        import json
        from datetime import datetime, timezone
        from unittest.mock import patch
        from app import get_weekend, update_weekend_configs_bulk, weekend_config_revision

        year = datetime.now(timezone.utc).year + 1
        wid1, wid2, wid3 = ("bulk-future-1-ci", "bulk-future-2-ci", "bulk-closed-3-ci")
        old_manual = f"{year}-02-28T09:45:00+00:00"
        initial = [
            {
                "id":wid1, "label":"GP Malaisie de test", "date":f"{year}-03-30",
                "season_year":year, "sessions":[], "deadline_utc":"",
                "auto_publish":True,"cancelled":False,
            },
            {
                "id":wid2, "label":"GP Portugal de test", "date":f"{year}-04-01",
                "season_year":year, "sessions":[], "deadline_utc":old_manual,
                "deadline_source":"manual","auto_publish":False,"cancelled":False,
            },
            {
                "id":wid3, "label":"GP historique clos de test", "date":f"{year}-03-22",
                "season_year":year, "sessions":[], "deadline_utc":"",
                "auto_publish":True,"cancelled":False,
            }
        ]
        with engine.begin() as conn:
            for item in initial:
                conn.execute(text("""
                    INSERT INTO weekends
                    (weekend_id,config_json,closed_at,pronos_public_at,updated_at)
                    VALUES (:id,CAST(:config AS jsonb),:closed,NULL,NOW())
                    ON CONFLICT (weekend_id) DO UPDATE
                    SET config_json=EXCLUDED.config_json,
                        closed_at=EXCLUDED.closed_at,
                        pronos_public_at=NULL,updated_at=NOW()
                """), {
                    "id":item["id"], "config":json.dumps(item),
                    "closed": datetime.now(timezone.utc) if item["id"] == wid3 else None
                })

        def event(external_id, name, day):
            return {
                "external_id":external_id, "name":name,
                "date":f"{year}-03-{day:02d}",
                "sessions":[
                    {"label":"FP1","utc":f"{year}-03-{day-2:02d}T10:00:00+00:00"},
                    {"label":"Q1","utc":f"{year}-03-{day-1:02d}T10:30:00+00:00"},
                    {"label":"Course","utc":f"{year}-03-{day:02d}T11:00:00+00:00"},
                ]
            }
        ev1,ev2,ev3=event("pul-m","Official Malaysian GP",15),event(
            "pul-p","Official Portuguese GP",22),event("pul-c","Official Historical GP",29)
        events=[ev1,ev2,ev3]
        uri=f"/admin/calendar/sync_motogp?season={year}"
        visitor=app.test_client()
        self.assertEqual(visitor.get(uri).status_code,302)
        player=self.client
        self.assertEqual(player.get(uri).status_code,403)

        admin=app.test_client()
        with admin.session_transaction() as st:
            st["is_admin"]=True
        with patch("v2_bulk_import.fetch_motogp_events",return_value=events):
            landing=admin.get(uri)
        self.assertEqual(landing.status_code,200)
        self.assertIn("Importer tous les horaires MotoGP",landing.get_data(as_text=True))
        with admin.session_transaction() as st:
            csrf=st["v2_csrf"]
        preview_data={
            "csrf":csrf,"action":"preview","season":str(year),
            "event_"+wid1:ev1["external_id"],
            "event_"+wid2:ev2["external_id"],
            "event_"+wid3:ev3["external_id"],
        }
        with patch("v2_bulk_import.fetch_motogp_events",return_value=events):
            self.assertEqual(admin.post(uri,data={**preview_data,"csrf":""}).status_code,403)
            page=admin.post(uri,data=preview_data)
        self.assertEqual(page.status_code,200,page.get_data(as_text=True)[:800])
        html=page.get_data(as_text=True)
        self.assertIn("Programmer automatiquement la clôture au départ de la Q1",html)
        self.assertIn("Clôture personnalisée",html) if "Clôture personnalisée" in html else None
        self.assertEqual(get_weekend(wid1)["date"],f"{year}-03-30")
        self.assertEqual(get_weekend(wid2)["deadline_utc"],old_manual)
        with admin.session_transaction() as st:
            token=st["bulk_motogp_preview"]
            self.assertEqual(token["year"],year)
            self.assertEqual(len(token["digest"]),64)
            self.assertLess(len(str(st)),3500)

        options={
            **preview_data, "action":"apply",
            "selected_gp":[wid1,wid2,wid3],
            "change_dates":"1","schedule_q1":"1","final_confirmation":"1"
        }
        with patch("v2_bulk_import.fetch_motogp_events",return_value=events), patch(
            "app.save_json",side_effect=OSError("Render read-only filesystem")
        ):
            applied=admin.post(uri,data=options)
        self.assertEqual(applied.status_code,302)
        gp1,gp2,gp3=(get_weekend(wid1),get_weekend(wid2),get_weekend(wid3))
        self.assertEqual(gp1["date"],ev1["date"])
        self.assertEqual(len(gp1["sessions"]),3)
        self.assertEqual(gp1["deadline_utc"],ev1["sessions"][1]["utc"])
        self.assertEqual(gp1["deadline_source"],"motogp_q1")
        self.assertTrue(gp1["auto_publish"])
        self.assertEqual(gp2["date"],ev2["date"])
        self.assertEqual(gp2["deadline_utc"],old_manual)
        self.assertEqual(gp2["deadline_source"],"manual")
        self.assertEqual(gp3["date"],f"{year}-03-22")  # clos : date conservée
        self.assertEqual(gp3["deadline_utc"],"")
        self.assertEqual(len(gp3["sessions"]),3)
        with engine.connect() as conn:
            closed=conn.execute(text(
                "SELECT closed_at FROM weekends WHERE weekend_id=:w"
            ),{"w":wid3}).scalar_one()
            self.assertIsNotNone(closed)

        # Nouveau preview, validation d'écrasement express pour le futur manuel.
        with patch("v2_bulk_import.fetch_motogp_events",return_value=events):
            self.assertEqual(admin.post(uri,data=preview_data).status_code,200)
            override=admin.post(uri,data={
                **preview_data, "action":"apply", "selected_gp":[wid2],
                "schedule_q1":"1","override_manual":"1","final_confirmation":"1"
            })
        self.assertEqual(override.status_code,302)
        gp2=get_weekend(wid2)
        self.assertEqual(gp2["deadline_utc"],ev2["sessions"][1]["utc"])
        self.assertEqual(gp2["deadline_source"],"motogp_q1")

        # Modification frauduleuse d'association après preview => refus.
        with patch("v2_bulk_import.fetch_motogp_events",return_value=events):
            self.assertEqual(admin.post(uri,data=preview_data).status_code,200)
            forged=admin.post(uri,data={
                **preview_data, "action":"apply",
                "event_"+wid1:ev2["external_id"],
                "event_"+wid2:ev1["external_id"],
                "selected_gp":[wid1], "schedule_q1":"1","final_confirmation":"1"
            })
        self.assertEqual(forged.status_code,302)
        self.assertEqual(get_weekend(wid1)["date"],ev1["date"])

        # Une version obsolète d'un GP fait échouer TOUT le lot, sans update partiel.
        old1=get_weekend(wid1)
        old2=get_weekend(wid2)
        ok,_=update_weekend_configs_bulk([
            {"id":wid1,"updates":{"date":f"{year}-04-01"},
             "revision":weekend_config_revision(old1)},
            {"id":wid2,"updates":{"date":f"{year}-04-04"},
             "revision":"f"*64}
        ])
        self.assertFalse(ok)
        self.assertEqual(get_weekend(wid1)["date"],old1["date"])
        self.assertEqual(get_weekend(wid2)["date"],old2["date"])

    def test_unified_profile_and_legacy_dashboard_redirect(self):
        from urllib.parse import urlsplit, parse_qs

        page = self.client.get("/profile")
        self.assertEqual(page.status_code, 200)
        html = page.get_data(as_text=True)
        for label in ("Mon profil", "Enregistrer ma photo", "Changer mon mot de passe",
                      "MES POINTS", "MON CLASSEMENT", "GP CLASSÉS",
                      "DERNIER RÉSULTAT", "Mon bilan"):
            with self.subTest(section=label):
                self.assertIn(label, html)
        self.assertNotIn("Mon espace", html)
        self.assertNotIn("Mes pronostics", html)
        self.assertNotIn("v2-event-grid", html)
        self.assertNotIn("Clôture des pronostics", html)
        self.assertNotIn("v2-season-picker", html)
        self.assertNotIn("Calendrier MotoGP", html)

        old_link = self.client.get("/me")
        self.assertEqual(old_link.status_code, 302)
        self.assertEqual(urlsplit(old_link.headers["Location"]).path, "/profile")

        year_link = self.client.get("/me?season=2026")
        self.assertEqual(year_link.status_code, 302)
        parsed = urlsplit(year_link.headers["Location"])
        self.assertEqual(parsed.path, "/profile")
        self.assertEqual(parse_qs(parsed.query)["season"], ["2026"])

        fresh = app.test_client()
        login = fresh.post("/login", data={
            "name": "IntegrationTest", "secret": "secure-test-123"
        })
        self.assertEqual(login.status_code, 302)
        self.assertEqual(urlsplit(login.headers["Location"]).path, "/profile")
        self.assertEqual(fresh.get("/profile").status_code, 200)

    def test_player_changes_password_and_revokes_other_sessions(self):
        from werkzeug.security import check_password_hash

        added, message = add_participant("PasswordChangeCI", "initial-pass-123")
        self.assertTrue(added, message)
        user = app.test_client()
        other_device = app.test_client()
        def do_login(client, password):
            return client.post("/login", data={"name":"PasswordChangeCI", "secret":password})
        self.assertEqual(do_login(user, "initial-pass-123").status_code, 302)
        self.assertEqual(do_login(other_device, "initial-pass-123").status_code, 302)

        page = user.get("/profile")
        self.assertEqual(page.status_code, 200)
        html = page.get_data(as_text=True)
        self.assertIn("Changer mon mot de passe", html)
        self.assertIn('name="current_password"', html)
        self.assertIn('name="confirm_password"', html)
        with user.session_transaction() as st:
            csrf = st["v2_csrf"]

        url = "/profile"
        payload = {
            "csrf": csrf, "operation": "change_password",
            "current_password": "initial-pass-123",
            "new_password": "longer-secret-456",
            "confirm_password": "longer-secret-456"
        }
        initial_hash = get_participant_by_input("PasswordChangeCI")["secret_hash"]
        invalids = (
            {**payload, "csrf": ""},
            {**payload, "current_password": "wrong-old-secret"},
            {**payload, "confirm_password": "typo-secret"},
            {**payload, "new_password": "short", "confirm_password": "short"},
            {**payload, "new_password": "initial-pass-123",
             "confirm_password": "initial-pass-123"},
        )
        for i, bad in enumerate(invalids):
            with self.subTest(invalid=i):
                response = user.post(url, data=bad)
                self.assertEqual(response.status_code, 403 if i == 0 else 302)
                self.assertEqual(get_participant_by_input("PasswordChangeCI")["secret_hash"],
                                 initial_hash)

        self.assertEqual(user.post(url, data=payload).status_code, 302)
        new_hash = get_participant_by_input("PasswordChangeCI")["secret_hash"]
        self.assertNotEqual(initial_hash, new_hash)
        self.assertTrue(check_password_hash(new_hash, "longer-secret-456"))
        self.assertNotIn("longer-secret-456", new_hash)
        self.assertEqual(user.get("/profile").status_code, 200)
        self.assertEqual(other_device.get("/profile").status_code, 302)

        old_login = app.test_client()
        self.assertEqual(old_login.post("/login", data={
            "name": "PasswordChangeCI", "secret": "initial-pass-123"
        }).headers["Location"].rsplit("/",1)[-1], "login")
        self.assertEqual(do_login(old_login, "longer-secret-456").status_code, 302)
        self.assertEqual(old_login.get("/profile").status_code, 200)

    def test_admin_resets_password_without_old_secret(self):
        from werkzeug.security import check_password_hash

        added, message = add_participant("AdminRecoveryCI", "original-secret-123")
        self.assertTrue(added, message)
        target = get_participant_by_input("AdminRecoveryCI")
        pid = target["id"]
        old_hash = target["secret_hash"]
        player = app.test_client()
        self.assertEqual(player.post("/login", data={
            "name":"AdminRecoveryCI","secret":"original-secret-123"
        }).status_code,302)
        admin = app.test_client()
        with admin.session_transaction() as st:
            st["is_admin"] = True
        self.assertEqual(admin.get("/admin/participants").status_code,200)
        with admin.session_transaction() as st:
            token = st["v2_csrf"]
        url = f"/admin/participants/{pid}/reset_secret"

        request_data = {
            "csrf": token, "new_secret": "recovery-secret-456",
            "new_secret_confirm": "recovery-secret-456"
        }
        self.assertEqual(app.test_client().post(url, data=request_data).status_code, 401)
        self.assertEqual(admin.post(url, data={
            "new_secret": "recovery-secret-456",
            "new_secret_confirm": "recovery-secret-456"
        }).status_code,403)
        self.assertEqual(admin.post(url, data={
            **request_data, "new_secret_confirm": "different-secret"
        }).status_code,302)
        self.assertEqual(admin.post(url, data={
            **request_data, "new_secret": "1234", "new_secret_confirm": "1234"
        }).status_code,302)
        self.assertEqual(get_participant_by_input("AdminRecoveryCI")["secret_hash"],old_hash)

        self.assertEqual(admin.post(url, data=request_data).status_code,302)
        new_hash = get_participant_by_input("AdminRecoveryCI")["secret_hash"]
        self.assertNotEqual(new_hash,old_hash)
        self.assertTrue(check_password_hash(new_hash,"recovery-secret-456"))
        self.assertEqual(player.get("/profile").status_code,302)

        old = app.test_client()
        self.assertEqual(old.post("/login",data={
            "name":"AdminRecoveryCI","secret":"original-secret-123"
        }).headers["Location"].rsplit("/",1)[-1],"login")
        self.assertEqual(old.post("/login",data={
            "name":"AdminRecoveryCI","secret":"recovery-secret-456"
        }).status_code,302)
        self.assertEqual(old.get("/profile").status_code,200)

        # Les autres actions d'administration des joueurs sont aussi protegees.
        self.assertEqual(admin.post(
            f"/admin/participants/{pid}/toggle",data={}
        ).status_code,403)
        self.assertEqual(admin.post("/admin/participants",data={
            "pseudo":"BadCsrf","secret":"password-long"
        }).status_code,403)

    def test_photo_persists_in_database(self):
        picture=Image.new("RGB",(100,100),"blue")
        buffer=io.BytesIO()
        picture.save(buffer,format="PNG")
        buffer.seek(0)
        r=self.client.post("/profile",data={"csrf":self.csrf,"photo":(buffer,"demo.png")},
                           content_type="multipart/form-data")
        self.assertEqual(r.status_code,302)
        pid=get_participant_by_input("IntegrationTest")["id"]
        avatar=self.client.get("/avatar/"+str(pid))
        self.assertEqual(avatar.status_code,200)
        self.assertEqual(avatar.mimetype,"image/jpeg")

    def test_schema_and_notification_delivery_queue(self):
        for name in ("seasons","bonus_reviews","player_photos","push_messages","web_push_subscriptions"):
            with engine.connect() as c:
                exists=c.execute(text("SELECT to_regclass(:name)"),{"name":name}).scalar()
            self.assertIsNotNone(exists,name)
        self.assertEqual(self.client.get("/notifications").status_code,200)
        with self.client.session_transaction() as st:
            st["is_admin"]=True
        r=self.client.post("/admin/notifications",data={
            "csrf":self.csrf,"title":"Message de test","body":"Message en base","target":"notifications","recipients":"selected",
            "player_ids":[str(get_participant_by_input("IntegrationTest")["id"])]
        })
        self.assertEqual(r.status_code,302)
        content=self.client.get("/notifications").get_data(as_text=True)
        self.assertIn("Message de test",content)

    def test_automatic_deadline_and_publication(self):
        from datetime import datetime, timedelta
        from zoneinfo import ZoneInfo
        from app import is_weekend_closed, is_pronos_public
        # Un GP isolé dans la DB de CI : ne toucher à aucune saison réelle.
        with self.client.session_transaction() as st:
            st["is_admin"] = True
        year = 2044
        self.client.post("/admin/seasons", data={
            "csrf":self.csrf,"action":"add","year":str(year)
        })
        self.client.post("/admin/seasons", data={
            "csrf":self.csrf,"action":"gp","year":str(year),
            "label":"GP Test CI","date":"2044-04-20"
        })
        wid = "2044-gp-test-ci"
        paris = ZoneInfo("Europe/Paris")
        past = (datetime.now(paris) - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M")
        future = (datetime.now(paris) + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M")
        # Un GP clos n'est pas automatiquement public si cette option est désactivée.
        response = self.client.post(f"/admin/w/{wid}/schedule", data={
            "csrf":self.csrf,"deadline":past,"sessions":"","auto_publish":""
        })
        self.assertEqual(response.status_code, 302)
        self.assertTrue(is_weekend_closed(wid))
        self.assertFalse(is_pronos_public(wid))
        self.assertEqual(self.client.get(f"/w/{wid}/public/pronos").status_code,403)
        # Publication automatique sans action programmée/cron.
        response = self.client.post(f"/admin/w/{wid}/schedule", data={
            "csrf":self.csrf,"deadline":past,"sessions":"","auto_publish":"1"
        })
        self.assertEqual(response.status_code,302)
        self.assertTrue(is_pronos_public(wid))
        self.assertEqual(self.client.get(f"/w/{wid}/public/pronos").status_code,200)
        # Corriger l'heure APRÈS la date initiale permet de réouvrir les pronos.
        response = self.client.post(f"/admin/w/{wid}/schedule", data={
            "csrf":self.csrf,"deadline":future,"sessions":"","auto_publish":"1"
        })
        self.assertEqual(response.status_code,302)
        self.assertFalse(is_weekend_closed(wid))
        self.assertFalse(is_pronos_public(wid))
        # Correction administrative de la date même après l'ancienne échéance.
        response=self.client.post(f"/admin/calendar/{wid}/update",data={
            "csrf":self.csrf, "date":"2044-04-21","label":"GP Test CI - corrigé"
        })
        self.assertEqual(response.status_code,302)
        self.assertIn("corrigé", self.client.get("/admin/calendar").get_data(as_text=True))

    def test_ambiguous_dst_times_are_rejected(self):
        from v2_features import parse_paris_local
        with self.assertRaises(ValueError):
            parse_paris_local("2026-10-25T02:30")
        with self.assertRaises(ValueError):
            parse_paris_local("2026-03-29T02:30")

    def test_season_teams_are_isolated(self):
        from app import get_teams_for_season, OFFICIAL_TEAMS
        with self.client.session_transaction() as st:
            st["is_admin"] = True
        self.client.post("/admin/seasons", data={
            "csrf":self.csrf,"action":"add","year":"2046"
        })
        self.assertIn(OFFICIAL_TEAMS[0], get_teams_for_season(2046, kind="official"))
        r=self.client.post("/admin/teams",data={
            "csrf":self.csrf,"season":"2046","action":"add",
            "team":"Integration Racing MotoGP","kind":"satellite"
        })
        self.assertEqual(r.status_code,302)
        self.assertIn("Integration Racing MotoGP",get_teams_for_season(2046,kind="satellite"))
        self.assertNotIn("Integration Racing MotoGP",get_teams_for_season(2026))
        r=self.client.post("/admin/teams",data={
            "csrf":self.csrf,"season":"2046","action":"toggle",
            "team":"Integration Racing MotoGP"
        })
        self.assertEqual(r.status_code,302)
        self.assertNotIn("Integration Racing MotoGP",get_teams_for_season(2046))
        self.assertIn("Integration Racing MotoGP",
                      get_teams_for_season(2046,active_only=False))

    def test_season_riders_are_isolated(self):
        from app import get_riders_for_season, RIDERS
        with self.client.session_transaction() as st:
            st["is_admin"] = True
        self.client.post("/admin/seasons", data={
            "csrf":self.csrf, "action":"add", "year":"2045"
        })
        copied = get_riders_for_season(2045)
        self.assertEqual(set(copied), set(RIDERS))
        resp = self.client.post("/admin/riders", data={
            "csrf":self.csrf, "season":"2045",
            "action":"add", "rider":"#99 Integration Pilot"
        })
        self.assertEqual(resp.status_code,302)
        self.assertIn("#99 Integration Pilot", get_riders_for_season(2045))
        self.assertNotIn("#99 Integration Pilot", get_riders_for_season(2026))
        target=RIDERS[0]
        self.client.post("/admin/riders", data={
            "csrf":self.csrf, "season":"2045", "action":"toggle", "rider":target
        })
        self.assertNotIn(target, get_riders_for_season(2045))
        self.assertIn(target, get_riders_for_season(2045, active_only=False))
        self.assertIn(target, get_riders_for_season(2026))

    def test_new_season_created_and_archived(self):
        with self.client.session_transaction() as st: st["is_admin"]=True
        r=self.client.post("/admin/seasons",data={"csrf":self.csrf,"action":"add","year":"2027"})
        self.assertEqual(r.status_code,302)
        self.assertEqual(self.client.get("/admin/seasons").status_code,200)
        self.assertEqual(self.client.get("/?season=2027").status_code,200)
        self.assertEqual(self.client.get("/championnat?season=2027").status_code,200)
        r=self.client.post("/admin/seasons",data={"csrf":self.csrf,"action":"archive","year":"2027","archived":"1"})
        self.assertEqual(r.status_code,302)
        with engine.connect() as c:
            state=c.execute(text("SELECT archived FROM seasons WHERE year=2027")).scalar()
        self.assertTrue(state)


if __name__=="__main__":
    unittest.main()
