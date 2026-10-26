import os
import unittest
from datetime import datetime, timezone, timedelta

os.environ.pop("DATABASE_URL", None)
from app import app
from v2_features import deadline_from_weekend, parse_paris_local, race_center


class V2PagesTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()

    def test_private_pages_redirect_anonymous_to_login(self):
        from urllib.parse import urlsplit
        for url in ("/", "/classement", "/results_by_race",
                    "/championnat/public", "/me", "/profile", "/avatar/1"):
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 302)
                self.assertEqual(urlsplit(response.headers["Location"]).path, "/login")

    def test_login_and_technical_pages_remain_available(self):
        for url in ("/login", "/healthz",
                    "/manifest.webmanifest", "/service-worker.js"):
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 200, response.get_data(as_text=True)[:400])
        # L'administration doit rester accessible sans session, mais renvoie
        # 500 dans le job CI qui ne configure volontairement aucun admin.
        from app import admin_enabled
        self.assertEqual(self.client.get("/admin/login").status_code,
                         200 if admin_enabled() else 500)

    def test_login_page_hides_private_menu_for_guest(self):
        html = self.client.get("/login").get_data(as_text=True)
        self.assertNotIn('class="header-nav"', html)
        self.assertIn("Connexion", html)

    def test_admin_can_browse_with_admin_session_only(self):
        with self.client.session_transaction() as state:
            state["is_admin"] = True
        for url in ("/", "/classement", "/results_by_race"):
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 200, response.get_data(as_text=True)[:400])

    def test_template_smoke_race_center(self):
        with app.test_request_context("/"):
            from flask import render_template
            html = render_template("_race_center.html", rc={
                "id":"qatar", "name":"GP du Qatar", "deadline_iso":"2026-10-25T12:00:00+00:00",
                "session_label":"Q1", "session_iso":"2026-10-25T12:30:00+00:00"
            })
            self.assertIn("Clôture des pronostics", html)
            self.assertIn("GP du Qatar", html)
            html=render_template("profile.html", name="Alex", person={"id":1,"pseudo":"Alex"},
               csrf="test", points=12.5, rank=2, gp_count=1,
               last_result={"name":"GP Test","points":12.5}, season_year=2026,
               # Même si un appelant injecte accidentellement le calendrier,
               # le template profil ne doit jamais l'afficher.
               weekends=[{"id":"futur-ci", "label":"GP du futur à cacher", "date":"2026-11-01"}],
               next_gp={"name":"Race Center caché", "deadline_iso":"2026-10-31T15:00:00+00:00"})
            self.assertIn("Mon profil", html)
            self.assertIn("Enregistrer ma photo", html)
            self.assertIn("Changer mon mot de passe", html)
            self.assertIn('name="current_password"', html)
            self.assertIn('name="new_password"', html)
            self.assertIn('name="confirm_password"', html)
            self.assertIn("MES POINTS", html)
            self.assertIn("MON CLASSEMENT", html)
            self.assertIn("Mon bilan", html)
            self.assertIn("GP CLASSÉS", html)
            self.assertIn("DERNIER RÉSULTAT", html)
            self.assertIn("GP Test", html)
            self.assertNotIn("Mes pronostics", html)
            self.assertNotIn("v2-event-grid", html)
            self.assertNotIn("GP du futur à cacher", html)
            self.assertNotIn("Race Center caché", html)
            self.assertNotIn("Clôture des pronostics", html)
            self.assertNotIn("Mon espace", html)
            # Les statistiques doivent être dans la page et non dans <title>.
            self.assertNotIn("<h2>", html.split("</title>")[0])
            self.assertIn("Mon bilan", html.split("</title>")[1])

    def test_local_time_paris_in_utc(self):
        utc=parse_paris_local("2026-10-25T11:30")
        self.assertEqual(utc.utcoffset(),timedelta())
        self.assertEqual(utc.hour,10)
        self.assertEqual(deadline_from_weekend({"deadline_utc":utc.isoformat()}),utc)
        with self.assertRaises(ValueError):
            parse_paris_local("2026-03-29T02:30")

    def test_no_automatic_close_without_explicit_deadline(self):
        self.assertIsNone(deadline_from_weekend({"date":"2026-10-25"}))
        self.assertIsNone(race_center([{"id":"a","label":"Test","date":"2026-10-25","sessions":[]}]))

if __name__ == "__main__":
    unittest.main()
