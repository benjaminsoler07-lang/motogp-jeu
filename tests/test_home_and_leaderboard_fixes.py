import os
import unittest
from unittest.mock import patch
from datetime import datetime, timezone, timedelta, date

os.environ.pop("DATABASE_URL", None)
from flask import render_template
from app import app, compute_points_breakdown
from season_standings import build_season_standings, individual_season_summary
from v2_features import _race_session_cards, _race_session_kind


class HomepageAndLeaderboardRegressionTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()
        self.gp = {
            "id": "test-gp", "label": "GP d'essai",
            "season_year": 2026, "date": "2026-10-20",
            "date_obj": date(2026, 10, 20),
            "cancelled": False, "bonus_questions": [],
        }
        self.pronos = {
            "Alice": {"pole": "#93 Marc Marquez", "gp_p1": "#93 Marc Marquez"},
            "Bob": {"pole": "#20 Fabio Quartararo", "gp_p1": "#20 Fabio Quartararo"},
        }
        self.results = {
            "pole": "#93 Marc Marquez", "q1": [],
            "sprint": [], "gp": ["#93 Marc Marquez", "#20 Fabio Quartararo", "#49 Fabio Di Giannantonio"],
            "bonus": {},
        }

    def test_race_sessions_use_actual_schedule_without_filling_in_missing_times(self):
        utc = datetime(2026, 11, 1, 10, 0, tzinfo=timezone.utc)
        sessions = [
            {"label": "Q1", "utc": utc.isoformat()},
            {"label": "Q2", "utc": (utc + timedelta(minutes=30)).isoformat()},
            {"label": "Sprint", "utc": (utc + timedelta(hours=5)).isoformat()},
            {"label": "Course", "utc": (utc + timedelta(days=1)).isoformat()},
        ]
        cards = _race_session_cards({"sessions": sessions})
        self.assertEqual([c["key"] for c in cards], ["qualif", "sprint", "gp"])
        self.assertEqual(cards[0]["iso"], utc.isoformat())
        self.assertEqual(cards[0]["date_label"], "01/11 · 11h00")
        self.assertEqual(cards[1]["date_label"], "01/11 · 16h00")
        self.assertEqual(cards[2]["date_label"], "02/11 · 11h00")
        self.assertEqual(_race_session_kind("GP"), "gp")
        self.assertEqual(_race_session_kind("Practice"), None)
        missing = _race_session_cards({"sessions": [{"label": "FP1", "utc": utc.isoformat()}]})
        self.assertTrue(all(card["iso"] is None for card in missing))

    def test_weekend_panel_contains_three_real_countdowns(self):
        cards = _race_session_cards({
            "sessions": [
                {"label": "Q1", "utc": "2026-11-01T10:00:00+00:00"},
                {"label": "Sprint", "utc": "2026-11-01T15:00:00+00:00"},
                {"label": "Course", "utc": "2026-11-02T10:00:00+00:00"},
            ]
        })
        with app.test_request_context("/"):
            html = render_template("_race_center.html", rc={
                "id": "malaisie", "name": "GP de Malaisie", "closed": False, "public": False,
                "played": False, "deadline_iso": None, "session_iso": None,
                "session_label": None, "session_cards": cards,
            })
        self.assertEqual(html.count('data-countdown-finished="Séance terminée"'), 3)
        self.assertIn("01/11 · 11h00", html)
        self.assertIn("Grand Prix", html)
        self.assertIn("data-countdown=", html)
        self.assertNotIn("neon-hero-art", html)

    def test_summary_and_general_leaderboard_use_identical_points(self):
        weekly = [dict(self.gp)]
        def fake_compute(prediction, result, w, player_name=None, reviews=None):
            pts = 9.5 if player_name == "Alice" else 6.0
            return {"qualif": {"total": 2}, "sprint": {"total": 0},
                    "gp": {"total": pts - 2}, "bonus": {"total": 0}, "total": pts}
        rows, gp_scored, rounds = build_season_standings(
            weekly, lambda _: self.results, lambda _: self.pronos,
            lambda _: {}, fake_compute,
        )
        self.assertEqual(len(gp_scored), 1)
        self.assertEqual(rounds[0]["scores"]["Alice"], 9.5)
        self.assertEqual(individual_season_summary(rows, "ALICE", lambda n: n.lower()),
                         {"points": 9.5, "rank": 1})
        self.assertEqual(individual_season_summary(rows, "Bob", lambda n: n.lower()),
                         {"points": 6.0, "rank": 2})

    def test_populated_classement_page_no_longer_crashes(self):
        with self.client.session_transaction() as sess:
            sess["is_admin"] = True
        with (
            patch("app.get_sorted_weekends", return_value=[self.gp]),
            patch("app.load_results", return_value=self.results),
            patch("app.get_latest_pronos_by_player_for_weekend", return_value=self.pronos),
            patch("app.load_bonus_reviews", return_value={}),
        ):
            response = self.client.get("/classement?season=2026")
        self.assertEqual(response.status_code, 200, response.get_data(as_text=True)[-900:])
        html = response.get_data(as_text=True)
        self.assertIn("Le championnat sous la loupe", html)
        self.assertIn("gp-championship-note", html)
        self.assertNotIn("Performances sur les 5 derniers GP", html)
        self.assertNotIn("gp-season-charts", html)
        self.assertIn("Alice", html)

    def test_result_gp_picker_is_unique_and_podium_photos_are_visible(self):
        rows = []
        for n in ("Alice", "Bob", "Charlie"):
            picks = {"pole": "#93 Marc Marquez", "gp_p1": "#93 Marc Marquez"}
            rows.append({
                "player": n, "p": picks,
                "pts": compute_points_breakdown(
                    picks, self.results, self.gp, player_name=n, reviews={}
                ),
            })
        with app.test_request_context("/results_by_race?gp=test-gp"):
            html = render_template(
                "results_by_race.html", name="Alice", selected=self.gp,
                weekends=[self.gp], results=self.results, rows=rows,
                notice=None, analysis=[], v2_avatar_ids={
                    "alice": 1, "bob": 2, "charlie": 3,
                }
            )
        self.assertIn("results-gp-details", html)
        self.assertNotIn("results-gp-nav", html)
        self.assertIn('class="avatar-v2 avatar-lg gp-podium-avatar"', html)
        self.assertIn("/avatar/1", html)
        self.assertIn("/avatar/2", html)
        self.assertIn("/avatar/3", html)

    def test_home_cards_show_actual_points_without_repeated_shortcuts(self):
        with app.test_request_context("/"):
            html = render_template("index.html", name="Alice", season_year=2026,
                                   years=[2026], race_center=None, upcoming=[],
                                   personal_summary={"points": 9.5, "rank": 2})
        self.assertIn("9,5 pts", html)
        self.assertIn("MON CLASSEMENT", html)
        self.assertIn("<sup>e</sup>", html)
        self.assertNotIn("v2-home-shortcuts", html)
        self.assertNotIn("neon-feature-card", html)


if __name__ == "__main__":
    unittest.main()
