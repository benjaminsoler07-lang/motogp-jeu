import os
from pathlib import Path
import unittest

os.environ.pop("DATABASE_URL", None)

from flask import render_template
from app import app


ROOT = Path(__file__).resolve().parents[1]


class NeonDesignTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()
        self.rc = {
            "id": "malaisie", "name": "GP de Malaisie", "closed": False,
            "public": False, "played": True,
            "deadline_iso": "2026-11-01T03:00:00+00:00",
            "session_iso": "2026-10-31T08:00:00+00:00",
            "session_label": "Q1",
        }

    def test_neon_css_and_original_illustration_are_served(self):
        css = self.client.get("/static/neon-v3.css")
        self.assertEqual(css.status_code, 200)
        text = css.get_data(as_text=True)
        self.assertIn("--neon-cyan", text)
        self.assertIn("neon-bottom-nav", text)
        self.assertIn("prefers-reduced-motion", text)
        asset = self.client.get("/static/neon-race-bike.svg")
        self.assertEqual(asset.status_code, 200)
        self.assertIn("<svg", asset.get_data(as_text=True))

    def test_dashboard_renders_live_gp_controls_with_no_fabricated_scores(self):
        with app.test_request_context("/?season=2026"):
            html = render_template(
                "index.html", name="TestUser", season_year=2026,
                years=[2026], race_center=self.rc, upcoming=[],
            )
        self.assertIn("La course commence ici.", html)
        self.assertIn("Le prochain Grand Prix", html)
        self.assertIn("GP de Malaisie", html)
        self.assertIn("v3-racecenter", html)
        self.assertIn("v3-session-countdowns", html)
        self.assertNotIn("neon-hero-art", html)
        self.assertIn("neon-home-summary", html)
        self.assertNotIn("v2-home-shortcuts", html)
        self.assertIn("Prono championnat", html)
        self.assertIn("/w/malaisie/pronos", html)
        self.assertIn("neon-bottom-nav", html)
        self.assertIn("neon-v3.css", html)
        self.assertIn("neon-refinement.css", html)
        self.assertNotIn("148 pts", html)
        self.assertNotIn("10,5 pts", html)

    def test_closed_and_public_states_keep_original_permissions(self):
        for public in (False, True):
            with self.subTest(public=public), app.test_request_context("/"):
                rc = dict(self.rc, closed=True, public=public)
                html = render_template("index.html", name="TestUser", season_year=2026,
                                       years=[2026], race_center=rc, upcoming=[])
                self.assertNotIn('href="/w/malaisie/pronos"', html)
                if public:
                    self.assertIn('href="/w/malaisie/public/pronos"', html)
                else:
                    self.assertIn("Pronostics fermés", html)

    def test_existing_pages_still_share_the_theme(self):
        with app.test_request_context("/"):
            for name in (
                "pronos.html", "results_by_race.html", "public_pronos.html",
                "classement_general.html", "championnat.html",
                "profile.html", "admin_home.html", "admin_weekend.html",
            ):
                self.assertIsNotNone(app.jinja_env.get_template(name))
            base = render_template("base.html", name="TestUser")
        self.assertIn("neon-v3", base)
        self.assertIn('aria-label="Navigation mobile"', base)
        self.assertIn("Dashboard admin", (ROOT / "templates" / "base.html").read_text(encoding="utf-8"))
        self.assertIn("neon-v3.css", base)

    def test_scoring_and_database_code_are_not_part_of_redesign(self):
        css = (ROOT / "static" / "neon-v3.css").read_text(encoding="utf-8")
        self.assertNotIn("DATABASE_URL", css)
        self.assertNotIn("compute_points_breakdown", css)


if __name__ == "__main__":
    unittest.main()
