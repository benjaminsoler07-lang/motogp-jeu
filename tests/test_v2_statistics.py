"""Tests purs des statistiques PronoGP (aucune écriture en base)."""
import unittest

from v2_statistics import _answer, build_comparison


class StatisticsTests(unittest.TestCase):
    def test_q1_order_is_ignored(self):
        a = {"q1_1": "#1 Martin", "q1_2": "#93 Marquez"}
        b = {"q1_1": "#93 Marquez", "q1_2": "#1 Martin"}
        self.assertEqual(_answer(a, "q1"), _answer(b, "q1"))

    def test_consensus_outliers_and_similarity(self):
        pronos = {
            "Benjam": {
                "pole": "#63 Bagnaia", "q1_1": "#1 Martin", "q1_2": "#93 Marquez",
                "gp_p1": "#1 Martin", "bonus": {"b1": "Oui"},
            },
            "Alex810": {
                "pole": "#63 Bagnaia", "q1_1": "#93 Marquez", "q1_2": "#1 Martin",
                "gp_p1": "#1 Martin", "bonus": {"b1": "Oui"},
            },
            "Goodspeed": {
                "pole": "#63 Bagnaia", "q1_1": "#1 Martin", "q1_2": "#93 Marquez",
                "gp_p1": "#93 Marquez", "bonus": {"b1": "Non"},
            },
        }
        c = build_comparison(pronos, [{"id": "b1", "label": "Une Ducati sur le podium ?"}])
        self.assertEqual(c["players"], ["Alex810", "Benjam", "Goodspeed"])
        self.assertEqual(c["consensus_count"], 2)
        self.assertEqual(c["difference_count"], 2)
        self.assertEqual(c["total_fields"], 9)
        self.assertTrue(any(x["player"] == "Goodspeed" and x["field"] == "Grand Prix · P1"
                            for x in c["outliers"]))
        best = c["similarities"][0]
        self.assertEqual({best["left"], best["right"]}, {"Benjam", "Alex810"})
        self.assertEqual(best["percent"], 100)

    def test_route_and_template_smoke_for_unpublished_gp(self):
        # Vérifie le rendu HTML, le menu et le verrouillage sans base de production.
        from app import app
        with app.test_client() as client:
            with client.session_transaction() as sess:
                sess["is_admin"] = True
            response = client.get("/statistiques")
            self.assertEqual(response.status_code, 200)
            self.assertIn(b"Statistiques", response.data)
            self.assertIn(b"Comparatif des", response.data)
            self.assertIn(b"/statistiques", response.data)

    def test_missing_answers_are_not_consensus(self):
        pronos = {"Alpha": {"pole": "#63"}, "Beta": {"pole": ""}}
        c = build_comparison(pronos, [])
        self.assertEqual(c["consensus_count"], 0)
        self.assertEqual(c["similarities"], [])
        self.assertEqual(c["matrix"][0]["cells"][0]["state"], "normal")


if __name__ == "__main__":
    unittest.main()
