import os
import re
import unittest
from flask import render_template

os.environ.pop("DATABASE_URL", None)
from app import app
from gp_insights import build_championship_insights


def standing(player, total, gps):
    return {"player": player, "total": total, "gps": gps,
            "q": 0, "s": 0, "gp": total, "bonus": 0}


def round_result(label, **scores):
    return {"id": label, "label": label, "scores": scores}


class ChampionshipNarrativeTests(unittest.TestCase):
    def test_no_data_generates_no_speculative_debrief(self):
        self.assertEqual(build_championship_insights([], []), {"comments": []})
        self.assertEqual(
            build_championship_insights([standing("Alice", 0, 0)], []),
            {"comments": []},
        )
        self.assertEqual(
            build_championship_insights([standing("Alice", 0, 0)],
                                        [round_result("GP test")]),
            {"comments": []},
        )

    def test_head_to_head_debrief_is_qualitative_not_a_scoreboard(self):
        insights = build_championship_insights(
            [standing("Alice", 14, 2), standing("Bob", 12, 2)],
            [round_result("GP France", Alice=8, Bob=7),
             round_result("GP Italie", Alice=6, Bob=5)],
        )
        self.assertEqual(set(insights), {"comments"})
        self.assertTrue(all(set(item) == {"icon", "title", "text"}
                            for item in insights["comments"]))
        full_text = " ".join(item["text"] for item in insights["comments"])
        self.assertIn("Alice", full_text)
        self.assertIn("Bob", full_text)
        self.assertIn("GP Italie", full_text)
        self.assertFalse(re.search(r"\b\d+(?:[.,]\d+)?\s*(?:pts?|points)\b", full_text))
        self.assertNotIn("Meilleure moyenne", full_text)

    def test_leader_change_is_grounded_in_previous_standings(self):
        details = build_championship_insights(
            [standing("Bob", 17, 3), standing("Alice", 16, 3)],
            [round_result("GP A", Alice=10, Bob=0),
             round_result("GP B", Alice=6, Bob=5),
             round_result("GP C", Alice=0, Bob=12)],
        )
        titles = [item["title"] for item in details["comments"]]
        self.assertIn("Changement de leader", titles)
        event = next(item for item in details["comments"]
                     if item["title"] == "Changement de leader")
        self.assertIn("Bob", event["text"])
        self.assertIn("Alice", event["text"])

    def test_momentum_and_consistency_require_enough_results(self):
        rounds = [
            round_result("GP A", Alice=4, Bob=5),
            round_result("GP B", Alice=4, Bob=5),
            round_result("GP C", Alice=8, Bob=5),
            round_result("GP D", Alice=9, Bob=5),
        ]
        insights = build_championship_insights(
            [standing("Alice", 25, 4), standing("Bob", 20, 4)],
            rounds,
        )
        titles = [item["title"] for item in insights["comments"]]
        self.assertIn("La dynamique du moment", titles)
        self.assertIn("La régularité fait la différence", titles)
        self.assertIn("Alice", next(c["text"] for c in insights["comments"]
                                    if c["title"] == "La dynamique du moment"))
        self.assertIn("Bob", next(c["text"] for c in insights["comments"]
                                  if c["title"] == "La régularité fait la différence"))

    def test_missing_gp_results_do_not_become_zero_for_momentum(self):
        data = build_championship_insights(
            [standing("Alice", 23, 3)],
            [round_result("GP A", Alice=4),
             round_result("GP B", Alice=6),
             round_result("GP C", Bob=2),
             round_result("GP D", Alice=13)],
        )
        titles = [item["title"] for item in data["comments"]]
        self.assertNotIn("La dynamique du moment", titles)

    def test_championship_page_is_written_commentary_with_podium_and_table(self):
        rows = [standing("Alice", 14, 2), standing("Bob", 12, 2)]
        analysis = build_championship_insights(
            rows, [round_result("GP France", Alice=8, Bob=7),
                   round_result("GP Italie", Alice=6, Bob=5)],
        )
        with app.test_request_context("/classement?season=2026"):
            html = render_template(
                "classement_general.html", name="Alice", season_year=2026,
                rows=rows, gp_scored=[], weekends=[], notice=None,
                championship_analysis=analysis,
            )
        self.assertIn("Le championnat sous la loupe", html)
        self.assertIn("gp-championship-editorial", html)
        self.assertIn("gp-championship-note", html)
        self.assertIn("La bataille pour la tête", html)
        self.assertIn("podium-card", html)
        self.assertIn('class="classement"', html)
        for obsolete in ("gp-season-cards", "gp-season-charts",
                         "gp-season-recent-table", "Meilleure moyenne",
                         "Écart de points entre les cinq premiers"):
            self.assertNotIn(obsolete, html)


if __name__ == "__main__":
    unittest.main()
