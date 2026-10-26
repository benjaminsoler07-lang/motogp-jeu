import os
import unittest

# Les tests ne doivent jamais se connecter à la base de production.
os.environ.pop("DATABASE_URL", None)

from app import (
    BONUS_POINTS_PER_QUESTION,
    evaluated_bonus_answer,
    compute_points_breakdown,
    bonus_detail,
    is_bonus_answer_correct,
)


class BonusReviewTests(unittest.TestCase):
    def setUp(self):
        self.question = {"id": "b1", "label": "Vainqueur ?", "type": "text"}
        self.player = "Jean-Luc"
        self.pred = "Fabio Di Gianantonio"
        self.official = "Fabio Di Giannantonio"

    def review(self, decision=True, official=None, predicted=None):
        return {("jean-luc", "b1"): {
            "decision": decision,
            "prediction_snapshot": self.pred if predicted is None else predicted,
            "official_snapshot": self.official if official is None else official,
        }}

    def test_automatic_keeps_correct_spelling_rules(self):
        self.assertTrue(is_bonus_answer_correct(self.question, "Énea Bastianini", "enea bastianini"))
        self.assertTrue(is_bonus_answer_correct(self.question, "Jean-Luc", "jean luc"))
        self.assertTrue(is_bonus_answer_correct(self.question, "Fabio Di-Giannantonio!", "fabio di giannantonio"))
        self.assertFalse(is_bonus_answer_correct(self.question, self.pred, self.official))

    def test_manual_accepts_typo_without_changing_prediction(self):
        self.assertEqual(
            evaluated_bonus_answer(self.question, self.pred, self.official,
                                   self.player, self.review()), (True, True))
        self.assertEqual(self.pred, "Fabio Di Gianantonio")

    def test_manual_refusal_overrides_automatic_acceptance(self):
        reviews = {("jean-luc", "b1"): {
            "decision": False, "prediction_snapshot": self.official,
            "official_snapshot": self.official,
        }}
        self.assertEqual(
            evaluated_bonus_answer(self.question, self.official, self.official,
                                   self.player, reviews), (False, True))

    def test_review_does_not_affect_other_player(self):
        self.assertEqual(
            evaluated_bonus_answer(self.question, self.pred, self.official,
                                   "Alex810", self.review()), (False, False))

    def test_official_answer_edit_invalidates_override(self):
        self.assertEqual(
            evaluated_bonus_answer(self.question, self.pred, "Marc Marquez",
                                   self.player, self.review()), (False, False))

    def test_prediction_edit_invalidates_override(self):
        self.assertEqual(
            evaluated_bonus_answer(self.question, "Autre réponse", self.official,
                                   self.player, self.review()), (False, False))

    def test_score_consistency_no_double_points(self):
        d = bonus_detail({"b1": self.pred}, {"b1": self.official},
                         [self.question], player_name=self.player, reviews=self.review())
        self.assertEqual(d["total"], BONUS_POINTS_PER_QUESTION)
        self.assertTrue(d["items"][0]["manual"])
        weekend = {"bonus_questions": [self.question]}
        prono = {"bonus": {"b1": self.pred}}
        results = {"bonus": {"b1": self.official}}
        full = compute_points_breakdown(
            prono, results, weekend, player_name=self.player,
            reviews=self.review())
        self.assertEqual(full["bonus"]["total"], BONUS_POINTS_PER_QUESTION)
        self.assertEqual(full["total"], BONUS_POINTS_PER_QUESTION)

    def test_auto_mode_remains_default(self):
        d = bonus_detail({"b1": self.pred}, {"b1": self.official},
                         [self.question], player_name=self.player)
        self.assertEqual(d["total"], 0.0)
        self.assertFalse(d["items"][0]["manual"])

    def test_motogp_scoring_unchanged(self):
        from app import podium_points, qualif_points
        race=["#93 Marc Marquez","#49 Fabio Di Giannantonio","#20 Fabio Quartararo"]
        self.assertEqual(podium_points(race,race,2,1,6,3),12.0)
        self.assertEqual(podium_points([race[1],race[0],race[2]],race,2,1,6,3),7.0)
        self.assertEqual(qualif_points(race[0], race[0], [race[1],race[2]], [race[1],race[2]]),3.0)


if __name__ == "__main__":
    unittest.main()
