import os
import unittest

os.environ.pop("DATABASE_URL", None)

from flask import render_template
from app import app
from gp_insights import build_public_insights


class RacingColorsTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()
        self.weekend = {
            "id": "gp-test", "label": "GP de test",
            "bonus_questions": [{"id": "bonus", "label": "Pluie ?", "type": "yes_no"}],
        }
        self.pronos = [
            {"player": "Alice", "updated_at": "2026-10-31",
             "p": {"pole": "Pilote M", "gp_p1": "Pilote A", "sprint_p1": "Pilote X",
                   "bonus": {"bonus": "Oui"}}},
            {"player": "Bob", "updated_at": "2026-10-31",
             "p": {"pole": "Pilote M", "gp_p1": "Pilote A", "sprint_p1": "Pilote Y",
                   "bonus": {"bonus": "Non"}}},
            {"player": "Charlie", "updated_at": "2026-10-31",
             "p": {"pole": "Pilote M", "gp_p1": "Pilote B", "sprint_p1": "Pilote Z",
                   "bonus": {"bonus": "Oui"}}},
        ]

    def test_identical_answers_have_same_color_and_unanimous_label(self):
        data = build_public_insights(self.pronos, self.weekend)
        pole = next(row for row in data["matrix"] if row["label"] == "Pole")
        self.assertTrue(pole["unanimous"])
        self.assertEqual(pole["verdict"], "Unanimité")
        self.assertEqual(len({cell["tone"] for cell in pole["cells"]}), 1)
        self.assertTrue(all(cell["status"] == "unanimous" for cell in pole["cells"]))
        self.assertTrue(all(cell["popular"] for cell in pole["cells"]))
        self.assertGreater(data["comparison"]["unanimous"], 0)

    def test_different_answers_are_visually_distinct_and_isolated_choices_not_scored(self):
        data = build_public_insights(self.pronos, self.weekend)
        gp = next(row for row in data["matrix"] if row["label"] == "GP · P1")
        self.assertEqual([c["status"] for c in gp["cells"]],
                         ["popular", "popular", "solo"])
        self.assertEqual(gp["cells"][0]["tone"], gp["cells"][1]["tone"])
        self.assertNotEqual(gp["cells"][0]["tone"], gp["cells"][2]["tone"])
        self.assertEqual(gp["cells"][2]["signal"], "Choix isolé")
        self.assertEqual(gp["cells"][2]["votes"], 1)
        self.assertEqual(data["comparison"]["contested"] >= 2, True)
        self.assertTrue(data["comparison"]["isolated"] >= 1)
        self.assertNotIn("points", gp["cells"][2])

    def test_missing_answers_remain_neutral_and_bonus_uses_same_palette_rules(self):
        data = build_public_insights(self.pronos, self.weekend)
        gp3 = next(row for row in data["matrix"] if row["label"] == "GP · P3")
        self.assertTrue(all(c["status"] == "missing" and c["tone"] == "neutral"
                            for c in gp3["cells"]))
        bonus = next(row for row in data["matrix"] if row["label"] == "Pluie ?")
        self.assertEqual(bonus["group_key"], "bonus")
        self.assertEqual(bonus["cells"][0]["tone"], bonus["cells"][2]["tone"])
        self.assertNotEqual(bonus["cells"][0]["tone"], bonus["cells"][1]["tone"])

    def test_comparison_template_uses_legible_colored_choice_cells(self):
        insights = build_public_insights(self.pronos, self.weekend)
        with app.test_request_context("/w/gp-test/public/pronos"):
            html = render_template("public_pronos.html", w=self.weekend, name="Alice",
                                   pronos=self.pronos, insights=insights,
                                   v2_avatar_ids={})
        for item in ("gp-comparison-highlights", "gp-comparison-cell",
                     "status-unanimous", "status-popular", "status-solo",
                     "group-sprint", "group-gp", "group-bonus",
                     "tone-cyan", "tone-coral", "SEUL CHOIX",
                     "Même sélection = même couleur", "racing-v4.css"):
            self.assertIn(item, html)
        self.assertIn("Pilote A", html)
        self.assertIn("Pilote B", html)

    def test_original_racing_art_is_served_without_affecting_results(self):
        svg = self.client.get("/static/racing-track-v4.svg")
        css = self.client.get("/static/racing-v4.css")
        self.assertEqual(svg.status_code, 200)
        self.assertEqual(css.status_code, 200)
        self.assertIn("<svg", svg.get_data(as_text=True))
        stylesheet = css.get_data(as_text=True)
        self.assertIn("racing-track-v4.svg", stylesheet)
        self.assertIn(".gp-comparison-table", stylesheet)
        self.assertIn("status-solo", stylesheet)
        self.assertIn("prefers-reduced-motion", stylesheet)


if __name__ == "__main__":
    unittest.main()
