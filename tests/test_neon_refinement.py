import os
import unittest

os.environ.pop("DATABASE_URL", None)

from flask import render_template
from app import app


class RaceCenterRefinementTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()
        self.rc = {
            "id": "malaisie",
            "name": "GP de Malaisie",
            "closed": False,
            "public": False,
            "played": False,
            "deadline_iso": "2026-11-01T02:00:00+00:00",
            "session_label": "Q1",
            "session_iso": "2026-10-31T09:00:00+00:00",
        }

    def render_race_center(self, **updates):
        with app.test_request_context("/"):
            return render_template("_race_center.html", rc=dict(self.rc, **updates))

    def test_live_information_survives_visual_redesign(self):
        html = self.render_race_center()
        for expected in (
            "GP de Malaisie", "RACE CENTER", "Clôture des pronostics",
            "data-countdown=\"2026-11-01T02:00:00+00:00\"",
            "data-countdown=\"2026-10-31T09:00:00+00:00\"",
            "Q1", "/w/malaisie/pronos", "Faire mon pronostic"
        ):
            self.assertIn(expected, html)
        self.assertIn("v3-session-countdowns", html)
        self.assertIn("v3-stage-qualif", html)
        self.assertIn("v3-stage-sprint", html)
        self.assertIn("v3-stage-gp", html)
        self.assertNotIn("neon-hero-art", html)
        self.assertNotIn("neon-race-bike.svg", html)

    def test_next_session_countdown_is_kept_even_with_identical_weekend_timer(self):
        session_time = "2026-11-01T10:00:00+00:00"
        slots = [
            {"key": "qualif", "title": "Qualifications", "iso": session_time,
             "date_label": "01/11 · 11h00"},
            {"key": "sprint", "title": "Sprint", "iso": None, "date_label": None},
            {"key": "gp", "title": "Grand Prix", "iso": None, "date_label": None},
        ]
        html = self.render_race_center(
            session_iso=session_time, session_label="Q1",
            deadline_iso=None, session_cards=slots,
        )
        self.assertEqual(html.count(f'data-countdown="{session_time}"'), 2)
        self.assertIn("v2-race-session-timer", html)
        self.assertIn("v3-session-countdowns", html)
        self.assertIn("01/11 · 11h00", html)

    def test_missing_schedule_stays_unknown_not_invented(self):
        html = self.render_race_center(deadline_iso=None, session_iso=None, session_label=None)
        self.assertIn("Clôture manuelle", html)
        self.assertIn("Horaire non renseigné", html)
        self.assertNotIn("data-countdown=", html)

    def test_closed_and_published_actions_unchanged(self):
        unpublished = self.render_race_center(closed=True, public=False, played=True)
        self.assertIn("Pronostics fermés", unpublished)
        self.assertIn("Ton pronostic est enregistré", unpublished)
        self.assertNotIn('href="/w/malaisie/pronos"', unpublished)
        self.assertNotIn('href="/w/malaisie/public/pronos"', unpublished)
        published = self.render_race_center(closed=True, public=True, played=True)
        self.assertIn('href="/w/malaisie/public/pronos"', published)
        self.assertIn("Voir les pronostics publics", published)

    def test_font_and_multi_color_mobile_theme(self):
        response = self.client.get("/static/neon-refinement.css")
        self.assertEqual(response.status_code, 200)
        css = response.get_data(as_text=True)
        for token in (
            "DM+Sans", "Plus+Jakarta+Sans", "--ref-cyan", "--ref-gold",
            "--ref-violet", "--ref-coral", ".v3-race-layout",
            ".neon-bottom-nav", "@media(max-width:720px)",
        ):
            self.assertIn(token, css)
        with app.test_request_context("/"):
            html = render_template("base.html", name="TestUser")
        self.assertIn("neon-refinement.css", html)
        self.assertIn("Navigation mobile", html)


if __name__ == "__main__":
    unittest.main()
