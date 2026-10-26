import unittest
from datetime import datetime, timezone
from unittest.mock import patch, Mock

from motogp_schedule import (
    normalise_event, extract_sessions, fetch_motogp_events,
    MotoGPAPIError, parse_external_datetime
)


def fixture_event(year=2026):
    return {
        "id": "official-uuid-malaysia",
        "name": "PETRONAS Grand Prix of Malaysia",
        "kind": "GP",
        "season": {"year": year},
        "broadcasts": [
            {"shortname": "FP1", "type": "SESSION",
             "category": {"name": "MotoGP", "acronym": "MGP"},
             "date_start": f"{year}-10-30T10:45:00+0800"},
            {"shortname": "Q1", "type": "SESSION",
             "category": {"name": "MotoGP", "acronym": "MGP"},
             "date_start": f"{year}-10-31T10:50:00+0800"},
            {"shortname": "SPR", "type": "SESSION",
             "category": {"name": "MotoGP", "acronym": "MGP"},
             "date_start": f"{year}-10-31T15:00:00+0800"},
            {"shortname": "RAC", "type": "SESSION",
             "category": {"name": "MotoGP", "acronym": "MGP"},
             "date_start": f"{year}-11-01T15:00:00+0800"},
            # Exclusions : autres catégories, médias et séances sans fuseau
            {"shortname": "Q1", "type": "SESSION",
             "category": {"name": "Moto2", "acronym": "MT2"},
             "date_start": f"{year}-10-31T08:00:00+0800"},
            {"shortname": "SHOW", "type": "MEDIA",
             "category": {"name": "MotoGP", "acronym": "MGP"},
             "date_start": f"{year}-10-29T12:00:00+0800"},
            {"shortname": "Q2", "type": "SESSION",
             "category": {"name": "MotoGP", "acronym": "MGP"},
             "date_start": f"{year}-10-31T12:00:00"},
        ]
    }


class MotoGPSyncTests(unittest.TestCase):
    def test_parses_timezone_and_filters_non_gp_sessions(self):
        event = normalise_event(fixture_event(), 2026)
        self.assertEqual(event["date"], "2026-11-01")
        self.assertEqual(event["external_id"], "official-uuid-malaysia")
        self.assertEqual([r["label"] for r in event["sessions"]],
                         ["FP1", "Q1", "Sprint", "Course"])
        self.assertEqual(event["sessions"][1]["utc"],
                         "2026-10-31T02:50:00+00:00")
        self.assertIsNone(parse_external_datetime("2026-10-31T10:50:00"))

    def test_never_assume_date_if_no_race_start(self):
        event = fixture_event()
        event["broadcasts"] = [b for b in event["broadcasts"]
                                if b.get("shortname") != "RAC"]
        self.assertIsNone(normalise_event(event, 2026))
        wrong = fixture_event()
        wrong["kind"] = "TEST"
        self.assertIsNone(normalise_event(wrong, 2026))

    def test_api_fetch_is_read_only_and_validates_shape(self):
        response = Mock()
        response.status_code = 200
        response.content = b"valid response"
        response.json.return_value = [
            {"kind": "TEST", "id": "ignored"},
            fixture_event(),
        ]
        with patch("motogp_schedule.requests.get", return_value=response) as getter:
            data = fetch_motogp_events(2026)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["date"], "2026-11-01")
        getter.assert_called_once()
        _, kwargs = getter.call_args
        self.assertEqual(kwargs["params"], {"seasonYear": 2026})
        self.assertFalse(kwargs["allow_redirects"])
        self.assertEqual(kwargs["timeout"], (4, 12))

        response.status_code = 403
        with patch("motogp_schedule.requests.get", return_value=response):
            with self.assertRaises(MotoGPAPIError):
                fetch_motogp_events(2026)

        response.status_code = 200
        response.json.return_value = {"unexpected": True}
        with patch("motogp_schedule.requests.get", return_value=response):
            with self.assertRaises(MotoGPAPIError):
                fetch_motogp_events(2026)

    def test_live_provider_reachability_diagnostic(self):
        # Diagnostic non bloquant : le fournisseur ne garantit pas l'accès public.
        import os
        if not os.environ.get("GITHUB_ACTIONS"):
            return
        try:
            import requests
            from motogp_schedule import API_BASE
            reply = requests.get(API_BASE + "/events",
                                 params={"seasonYear": 2026},
                                 timeout=(3, 8), allow_redirects=False,
                                 headers={"User-Agent": "PronoGP-Schedule/1.0",
                                          "Accept": "application/json"})
            data = reply.json() if reply.status_code == 200 else None
            size = len(data) if isinstance(data, list) else -1
            if isinstance(data, list):
                from motogp_schedule import normalise_event
                parsed = [normalise_event(x, 2026) for x in data]
                parsed = [x for x in parsed if x]
                print("PRONOGP_API_PROBE status=%s events=%s valid_gp=%s first=%s" %
                      (reply.status_code, size, len(parsed),
                       [(x["date"], x["name"], len(x["sessions"])) for x in parsed[:3]]))
            else:
                print("PRONOGP_API_PROBE status=%s events=%s" % (reply.status_code, size))
        except Exception as exc:
            print("PRONOGP_API_PROBE error=%s" % type(exc).__name__)

    def test_race_date_malaysia_in_paris(self):
        date = parse_external_datetime("2026-11-01T15:00:00+08:00")
        self.assertEqual(date, datetime(2026, 11, 1, 7, 0, tzinfo=timezone.utc))


if __name__ == "__main__":
    unittest.main()
