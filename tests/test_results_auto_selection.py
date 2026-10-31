import os
import unittest
from datetime import date, datetime, timezone
from unittest.mock import patch

os.environ.pop("DATABASE_URL", None)

from app import app
from results_selection import default_results_weekend


def race(ident, day, **extra):
    return {"id": ident, "label": f"GP {ident}", "date": day.isoformat(),
            "date_obj": day, "season_year": 2026, "cancelled": False,
            "bonus_questions": [], **extra}


class ResultsLandingTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()
        with self.client.session_transaction() as session:
            session["is_admin"] = True
        self.previous = race("australie", date(2026, 10, 25))
        self.current = race("malaisie", date(2026, 11, 1))
        self.upcoming = race("portugal", date(2026, 11, 8))
        self.calendar = [self.previous, self.current, self.upcoming]

    def test_during_gp_weekend_current_race_takes_priority(self):
        for when in (date(2026, 10, 30), date(2026, 10, 31),
                     date(2026, 11, 1)):
            with self.subTest(date=when):
                now = datetime(when.year, when.month, when.day, 12,
                               tzinfo=timezone.utc)
                chosen = default_results_weekend(self.calendar, now=now)
                self.assertEqual(chosen["id"], "malaisie")

    def test_after_gp_and_between_races_last_finished_is_shown(self):
        for when in (date(2026, 11, 2), date(2026, 11, 5)):
            with self.subTest(date=when):
                now = datetime(when.year, when.month, when.day, 12,
                               tzinfo=timezone.utc)
                self.assertEqual(default_results_weekend(self.calendar, now=now)["id"],
                                 "malaisie")

    def test_next_gp_does_not_displace_completed_one_while_future(self):
        now = datetime(2026, 10, 29, 12, tzinfo=timezone.utc)
        self.assertEqual(default_results_weekend(self.calendar, now=now)["id"],
                         "australie")

    def test_no_completed_race_yet_falls_back_to_first_upcoming(self):
        now = datetime(2026, 10, 1, 12, tzinfo=timezone.utc)
        self.assertEqual(default_results_weekend(self.calendar, now=now)["id"],
                         "australie")

    def test_cancelled_and_undated_races_are_skipped(self):
        abandoned = race("cancelled", date(2026, 10, 31))
        abandoned["cancelled"] = True
        now = datetime(2026, 10, 31, 12, tzinfo=timezone.utc)
        rows = [abandoned, {"id": "unknown"}, self.previous, self.current]
        self.assertEqual(default_results_weekend(rows, now=now)["id"], "malaisie")
        self.assertIsNone(default_results_weekend([abandoned, {"id": "unknown"}], now=now))

    def test_early_scheduled_practice_can_open_current_gp(self):
        opening = dict(self.current, sessions=[
            {"label": "FP1", "utc": "2026-10-29T08:00:00Z"},
            {"label": "GP", "utc": "2026-11-01T13:00:00Z"}
        ])
        now = datetime(2026, 10, 29, 11, tzinfo=timezone.utc)
        self.assertEqual(default_results_weekend([self.previous, opening], now=now)["id"],
                         "malaisie")

    def test_default_results_route_shows_selected_gp_and_same_dropdown(self):
        with (patch("app.get_sorted_weekends", return_value=self.calendar),
              patch("app.selected_season_year", return_value=2026),
              patch("app.default_results_weekend", return_value=self.current),
              patch("app.get_weekend", return_value=self.current),
              patch("app.load_results", return_value=None)):
            response = self.client.get("/results_by_race")
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("GP malaisie", html)
        self.assertIn('id="results-gp-list"', html)
        self.assertIn('id="results-gp-search"', html)
        self.assertIn('data-gp-label="GP australie"', html)
        self.assertIn('data-gp-label="GP portugal"', html)
        self.assertIn("Les résultats officiels de ce Grand Prix ne sont pas encore disponibles.", html)
        self.assertIn("Changer de Grand Prix", html)

    def test_manual_gp_selection_is_never_overridden(self):
        def get_weekend(ident):
            return next((row for row in self.calendar if row["id"] == ident), None)

        with (patch("app.get_sorted_weekends", return_value=self.calendar),
              patch("app.selected_season_year", return_value=2026),
              patch("app.get_weekend", side_effect=get_weekend),
              patch("app.load_results", return_value=None),
              patch("app.default_results_weekend") as choose):
            response = self.client.get("/results_by_race?gp=australie")
        self.assertEqual(response.status_code, 200)
        choose.assert_not_called()
        html = response.get_data(as_text=True)
        self.assertIn("GP australie", html)
        self.assertIn('data-gp-label="GP malaisie"', html)
        self.assertIn("Changer de Grand Prix", html)


if __name__ == "__main__":
    unittest.main()
