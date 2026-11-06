import unittest
from contextlib import contextmanager

from season_bulk import build_season_standings_batched
from season_standings import build_season_standings


class QueryRows(list):
    def fetchall(self):
        return list(self)

    def mappings(self):
        return self

    def all(self):
        return list(self)


class FakeConnection:
    def __init__(self):
        self.calls = []

    def execute(self, query, params):
        statement = str(query)
        self.calls.append((statement, params["ids"][:]))
        if "FROM results" in statement:
            return QueryRows([
                ("gp1", {"gp": ["Alice"], "bonus": {"known": "Oui", "stale": "Oui"}}),
                ("gp2", {"gp": ["Bob"], "bonus": {}}),
                ("gp3", {}),
            ])
        if "FROM pronos" in statement:
            return QueryRows([
                ("gp1", "Alice", {"pole": "Alice"}),
                ("gp1", "Bob", {"pole": "Bob"}),
                ("gp2", "Alice", {"pole": "Bob"}),
            ])
        if "FROM bonus_reviews" in statement:
            return QueryRows([{
                "weekend_id": "gp1", "player_norm": "alice",
                "question_id": "known", "decision": True,
                "prediction_snapshot": "Oui", "official_snapshot": "Oui",
                "reviewed_by": "admin", "reviewed_at": None,
            }])
        raise AssertionError("Unexpected SQL " + statement)


class FakeEngine:
    def __init__(self):
        self.conn = FakeConnection()

    @contextmanager
    def begin(self):
        yield self.conn


def fake_score(pred, results, weekend, *, player_name=None, reviews=None):
    assert "stale" not in results["bonus"]
    if weekend["id"] == "gp1":
        assert results["bonus"] == {"known": "Oui"}
        assert ("alice", "known") in reviews
    score = 3 if player_name == "Alice" else 1
    return {
        "qualif": {"total": 0}, "sprint": {"total": 0},
        "gp": {"total": score}, "bonus": {"total": 0},
        "total": score,
    }


class SeasonBulkTests(unittest.TestCase):
    def setUp(self):
        self.weekends = [
            {"id": "gp1", "label": "GP 1",
             "bonus_questions": [{"id": "known", "label": "Question"}]},
            {"id": "gp2", "label": "GP 2", "bonus_questions": []},
            {"id": "gp3", "label": "GP sans résultat", "bonus_questions": []},
            {"id": "gp4", "label": "GP annulé", "cancelled": True},
        ]

    def test_standings_identical_with_three_queries_and_no_stale_bonus(self):
        engine = FakeEngine()
        bootstrap_calls = []
        actual = build_season_standings_batched(
            self.weekends, engine, lambda: bootstrap_calls.append(True),
            fake_score, lambda wid: None, lambda wid: {}, lambda wid: {},
        )
        expected_results = {
            "gp1": {"gp": ["Alice"], "bonus": {"known": "Oui"}},
            "gp2": {"gp": ["Bob"], "bonus": {}},
        }
        predictions = {
            "gp1": {"Alice": {"pole": "Alice"}, "Bob": {"pole": "Bob"}},
            "gp2": {"Alice": {"pole": "Bob"}},
        }
        reviews = {"gp1": {
            ("alice", "known"): {"decision": True}
        }}
        expected = build_season_standings(
            self.weekends, lambda wid: expected_results.get(wid),
            lambda wid: predictions.get(wid, {}),
            lambda wid: reviews.get(wid, {}), fake_score,
        )
        self.assertEqual(actual, expected)
        self.assertEqual(bootstrap_calls, [True])
        self.assertEqual(len(engine.conn.calls), 3)
        self.assertEqual(engine.conn.calls[0][1], ["gp1", "gp2", "gp3"])
        self.assertEqual(engine.conn.calls[1][1], ["gp1", "gp2"])
        self.assertEqual(engine.conn.calls[2][1], ["gp1", "gp2"])

    def test_without_db_falls_back_to_original_loaders(self):
        results = {"gp1": {"gp": ["Alice"], "bonus": {"known": "Oui"}}}
        pronos = {"gp1": {"Alice": {"pole": "Alice"}}}
        review = {"gp1": {("alice", "known"): {"decision": True}}}
        loaders = (
            lambda wid: results.get(wid),
            lambda wid: pronos.get(wid, {}),
            lambda wid: review.get(wid, {}),
        )
        expected = build_season_standings(self.weekends, *loaders, fake_score)
        actual = build_season_standings_batched(
            self.weekends, None, lambda: self.fail("should not bootstrap"),
            fake_score, *loaders,
        )
        self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
