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
        for url in ("/login", "/admin/login", "/healthz",
                    "/manifest.webmanifest", "/service-worker.js"):
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 200, response.get_data(as_text=True)[:400])

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
            html=render_template("player_dashboard.html", name="Alex", person={"id":1,"pseudo":"Alex"},
               weekends=[], points=0.0, rank=None, gp_count=0, last_result=None, next_gp=None)
            self.assertIn("Bienvenue Alex",html)

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
