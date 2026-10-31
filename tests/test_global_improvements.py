import os
import unittest
from unittest.mock import patch, MagicMock

os.environ.pop("DATABASE_URL", None)
from flask import render_template
from app import app, compute_points_breakdown
from gp_insights import build_public_insights, build_championship_insights


class GlobalImprovementsTests(unittest.TestCase):
    def setUp(self):
        self.weekend = {
            "id": "gp-test", "label": "GP de test", "season_year": 2026,
            "bonus_questions": [{"id": "surprise", "label": "Chute ?", "type": "yes_no"}]
        }
        self.pronos = [
            {"player": "Alice", "updated_at": "2026-10-31", "p": {
                "pole": "#93 Marc Marquez", "gp_p1": "#49 Fabio Di Giannantonio",
                "gp_p2": "#93 Marc Marquez", "gp_p3": "#20 Fabio Quartararo",
                "sprint_p1": "#93 Marc Marquez", "bonus": {"surprise": "Oui"}}},
            {"player": "Bob", "updated_at": "2026-10-31", "p": {
                "pole": "#93 Marc Marquez", "gp_p1": "#93 Marc Marquez",
                "gp_p2": "#49 Fabio Di Giannantonio", "gp_p3": "#20 Fabio Quartararo",
                "sprint_p1": "#93 Marc Marquez", "bonus": {"surprise": "Non"}}}
        ]
        self.client = app.test_client()

    def test_comparison_is_transposed_and_includes_bonus_without_mutation(self):
        inputs = [row["p"].copy() for row in self.pronos]
        summary = build_public_insights(self.pronos, self.weekend)
        self.assertEqual([player["name"] for player in summary["players"]], ["Alice", "Bob"])
        self.assertEqual(summary["count"], 2)
        self.assertEqual(len(summary["matrix"]), 10)
        pole = next(line for line in summary["matrix"] if line["label"] == "Pole")
        self.assertEqual([cell["value"] for cell in pole["cells"]],
                         ["#93 Marc Marquez", "#93 Marc Marquez"])
        self.assertTrue(all(cell["popular"] for cell in pole["cells"]))
        bonus = next(line for line in summary["matrix"] if line["label"] == "Chute ?")
        self.assertEqual([cell["value"] for cell in bonus["cells"]], ["Oui", "Non"])
        self.assertEqual(self.pronos[0]["p"], inputs[0])
        self.assertEqual(self.pronos[1]["p"], inputs[1])

    def test_pre_analysis_is_grounded_and_supports_missing_predictions(self):
        data = build_public_insights(self.pronos, self.weekend)
        pole = next(d for d in data["distributions"] if d["title"] == "La pole")
        self.assertEqual(pole["votes"], 2)
        self.assertEqual(pole["bars"][0]["percent"], 100)
        self.assertIn("Unanimité", " ".join(c["text"] for c in data["comments"]))
        empty = build_public_insights([], self.weekend)
        self.assertEqual(empty["distributions"], [])
        self.assertEqual(empty["comments"], [])
        self.assertEqual(len(empty["matrix"]), 10)

    def test_public_template_contains_comparison_analysis_and_individual_details(self):
        with app.test_request_context("/w/gp-test/public/pronos"):
            html = render_template("public_pronos.html", w=self.weekend, name="Alice",
                                   pronos=self.pronos,
                                   insights=build_public_insights(self.pronos, self.weekend))
        self.assertIn("Comparaison des pronostics", html)
        self.assertIn("Analyse pré-course", html)
        self.assertIn("fiches individuelles", html)
        self.assertIn("gp-comparison-label", html)
        self.assertIn("Chute ?", html)
        self.assertIn("Alice", html)
        self.assertIn("Bob", html)

    def test_public_route_denies_access_before_closure_or_publication(self):
        with self.client.session_transaction() as state:
            state["is_admin"] = True
        for closed, published in ((False, False), (False, True), (True, False)):
            with self.subTest(closed=closed, published=published), (
                patch("app.engine", object()),
                patch("app.get_weekend", return_value=self.weekend),
                patch("app.is_weekend_closed", return_value=closed),
                patch("app.is_pronos_public", return_value=published)
            ):
                r = self.client.get("/w/gp-test/public/pronos")
                self.assertEqual(r.status_code, 403)

    def test_championship_analysis_consistent_with_existing_score_totals(self):
        results = {"pole": "#93 Marc Marquez", "q1": [],
                   "sprint": [], "gp": ["#49 Fabio Di Giannantonio", "#93 Marc Marquez",
                                          "#20 Fabio Quartararo"], "bonus": {}}
        pts = [compute_points_breakdown(p["p"], results, self.weekend)["total"] for p in self.pronos]
        scores = {p["player"]: pts[i] for i, p in enumerate(self.pronos)}
        ranking = sorted([
            {"player": player, "total": total, "gps": 1, "q": 0, "s": 0, "gp": 0, "bonus": 0}
            for player, total in scores.items()
        ], key=lambda row: -row["total"])
        insights = build_championship_insights(
            ranking, [{"id": "gp-test", "label": "GP de test", "scores": scores}])
        self.assertEqual(insights["cards"][0]["value"], 1)
        self.assertEqual(insights["cards"][1]["value"], 2)
        self.assertEqual(insights["bars"][0]["percent"], 100)
        self.assertEqual(sum(x["total"] for x in insights["bars"]), sum(pts))
        self.assertEqual(insights["recent"][0]["label"], "GP de test")
        self.assertEqual(build_championship_insights([], []), {
            "cards": [], "comments": [], "bars": [], "recent": []
        })

    def test_compact_header_keeps_navigation_and_account_controls(self):
        with self.client.session_transaction() as state:
            state["is_admin"] = True
        html = self.client.get("/").get_data(as_text=True)
        self.assertIn("gp-header-brand", html)
        self.assertIn("gp-user-menu", html)
        self.assertIn("gp-header-nav", html)
        self.assertIn("Résultats GP", html)
        self.assertNotIn(">Statistiques<", html)


if __name__ == "__main__":
    unittest.main()
