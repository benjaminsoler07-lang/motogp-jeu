import os
import unittest
from unittest.mock import patch

os.environ.pop("DATABASE_URL", None)

from flask import render_template
from app import app, build_gp_analysis, compute_points_breakdown


class ResultsGpFusionTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()
        self.weekend = {"id": "gp-test", "label": "GP Test", "season_year": 2026, "bonus_questions": []}
        self.results = {
            "pole": "#93 Marc Marquez",
            "q1": [],
            "sprint": ["#93 Marc Marquez", "#49 Fabio Di Giannantonio", "#20 Fabio Quartararo"],
            "gp": ["#49 Fabio Di Giannantonio", "#93 Marc Marquez", "#20 Fabio Quartararo"],
            "bonus": {},
        }
        alice = {
            "pole": "#93 Marc Marquez",
            "sprint_p1": "#93 Marc Marquez", "sprint_p2": "#49 Fabio Di Giannantonio",
            "sprint_p3": "#20 Fabio Quartararo",
            "gp_p1": "#49 Fabio Di Giannantonio", "gp_p2": "#93 Marc Marquez",
            "gp_p3": "#20 Fabio Quartararo",
        }
        bob = {
            "pole": "#20 Fabio Quartararo",
            "sprint_p1": "#49 Fabio Di Giannantonio", "sprint_p2": "#93 Marc Marquez",
            "sprint_p3": "#20 Fabio Quartararo",
            "gp_p1": "#93 Marc Marquez", "gp_p2": "#49 Fabio Di Giannantonio",
            "gp_p3": "#20 Fabio Quartararo",
        }
        self.rows = sorted([
            {"player": name, "p": prono,
             "pts": compute_points_breakdown(prono, self.results, self.weekend)}
            for name, prono in (("Alice", alice), ("Bob", bob))
        ], key=lambda r: r["pts"]["total"], reverse=True)

    def test_analysis_based_on_true_pronostics_and_does_not_change_points(self):
        before = [r["pts"]["total"] for r in self.rows]
        self.assertEqual(self.rows[0]["pts"]["gp"]["total"], 12.0)
        comments = build_gp_analysis(self.rows, self.results)
        self.assertGreaterEqual(len(comments), 4)
        self.assertLessEqual(len(comments), 6)
        messages = " ".join(item["text"] for item in comments)
        self.assertIn("Alice mène", messages)
        self.assertIn("Podium GP parfait pour Alice", messages)
        self.assertIn("1 joueur(s) sur 2", messages)
        self.assertEqual([r["pts"]["total"] for r in self.rows], before)

    def test_no_dubious_analysis_for_partial_or_missing_results(self):
        self.assertEqual(build_gp_analysis([], self.results), [])
        self.assertEqual(build_gp_analysis(self.rows, {"gp": ["#93"], "sprint": []}), [])

    def test_combined_page_has_official_results_podium_analysis_and_point_detail(self):
        with app.test_request_context("/results_by_race?gp=gp-test&season=2026"):
            html = render_template(
                "results_by_race.html", name="Alice", selected=self.weekend,
                weekends=[self.weekend], results=self.results, rows=self.rows,
                analysis=build_gp_analysis(self.rows, self.results), notice=None
            )
        for expected in (
            "Résultats officiels", "Podium des joueurs", "Analyse du GP",
            "Détail des points par joueur", "Podium GP parfait pour Alice",
            "Qualifications", "Questions bonus", "Alice", "Bob", "12.0"
        ):
            self.assertIn(expected, html)
        self.assertNotIn("Voir le classement", html)
        self.assertIn('class="classement gp-results-detail"', html)

    def test_legacy_gp_classement_url_redirects_to_results(self):
        with self.client.session_transaction() as state:
            state["is_admin"] = True
        with patch("app.get_weekend", return_value=self.weekend):
            response = self.client.get("/w/gp-test/classement")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/results_by_race?gp=gp-test", response.headers["Location"])


if __name__ == "__main__":
    unittest.main()
