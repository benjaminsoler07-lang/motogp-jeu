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

    def test_unified_profile_and_legacy_dashboard_redirect(self):
        from urllib.parse import urlsplit, parse_qs

        page = self.client.get("/profile")
        self.assertEqual(page.status_code, 200)
        html = page.get_data(as_text=True)
        for label in ("Mon profil", "Enregistrer ma photo", "MES POINTS",
                      "MON CLASSEMENT", "DERNIER RÉSULTAT", "Mes pronostics"):
            with self.subTest(section=label):
                self.assertIn(label, html)
        self.assertNotIn("Mon espace", html)

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
