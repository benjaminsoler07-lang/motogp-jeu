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
