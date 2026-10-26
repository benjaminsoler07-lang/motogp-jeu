import unittest
from datetime import datetime, timezone, timedelta

from motogp_bulk import (
    GP_ALIASES, normalized_words, suggest_motogp_event,
    batch_preview_digest, proposal_changes, q1_datetime
)


def fake_event(event_id, title, q1=None):
    q1 = q1 or (datetime.now(timezone.utc) + timedelta(days=22))
    return {
        "external_id": event_id,
        "name": title,
        "date": (q1 + timedelta(days=1)).date().isoformat(),
        "sessions": [
            {"label": "FP1", "utc": (q1-timedelta(days=1)).isoformat()},
            {"label": "Q1", "utc": q1.isoformat()},
            {"label": "Course", "utc": (q1+timedelta(days=1)).isoformat()}
        ]
    }


def fake_row(event, closed=False, deadline="", source="", year=None):
    return {
        "id": "malaisie", "label": "GP Malaisie",
        "year": year or int(event["date"][:4]),
        "revision": "a" * 64,
        "event": event,
        "closed": closed, "current_deadline": deadline,
        "deadline_source": source,
    }


class TestBulkGPImport(unittest.TestCase):
    def test_aliases_distinguish_austria_and_australia(self):
        e1 = fake_event("a", "Australian Grand Prix")
        e2 = fake_event("b", "Austrian Grand Prix")
        self.assertEqual(suggest_motogp_event("australie", [e1,e2]),e1)
        self.assertEqual(suggest_motogp_event("autriche", [e1,e2]),e2)
        self.assertEqual(suggest_motogp_event("malaisie", [e1,e2]),None)

    def test_malaysia_and_catalunya_aliases_ignore_sponsors(self):
        e1 = fake_event("malaysia", "PETRONAS GRAND PRIX OF MALAYSIA")
        e2 = fake_event("catalunya", "Monster Energy Gran Premi de Catalunya")
        self.assertEqual(suggest_motogp_event("malaisie", [e1,e2]),e1)
        self.assertEqual(suggest_motogp_event("catalogne", [e1,e2]),e2)
        self.assertEqual(len(GP_ALIASES),22)

    def test_ambiguous_events_never_auto_match(self):
        e1 = fake_event("1", "GP MALAYSIA")
        e2 = fake_event("2", "GP MALAYSIAN")
        self.assertIsNone(suggest_motogp_event("malaisie", [e1,e2]))
        self.assertEqual(normalized_words("Grand Prix d'Autriche"),"grand prix d autriche")

    def test_q1_is_exact_start_and_auto_rule(self):
        now = datetime.now(timezone.utc)
        q1=now+timedelta(days=5)
        event=fake_event("malaysia","GP OF MALAYSIA",q1=q1)
        row=fake_row(event)
        updated,result=proposal_changes(
            row,now=now,change_dates=True,schedule_q1=True,
            override_manual=False
        )
        self.assertEqual(result,"Clôture Q1 programmée")
        self.assertEqual(updated["deadline_utc"],q1.isoformat())
        self.assertEqual(updated["deadline_source"],"motogp_q1")
        self.assertEqual(updated["date"],event["date"])
        self.assertEqual(updated["sessions"],event["sessions"])
        self.assertEqual(q1_datetime(event),q1)

    def test_manual_deadline_never_overwritten_by_default(self):
        now=datetime.now(timezone.utc)
        event=fake_event("malaysia","GP OF MALAYSIA")
        row=fake_row(event,deadline=(now+timedelta(days=1)).isoformat(),source="manual")
        changes,result=proposal_changes(
            row,now=now,change_dates=True,schedule_q1=True,
            override_manual=False
        )
        self.assertNotIn("deadline_utc",changes)
        self.assertEqual(result,"Clôture personnalisée préservée")
        changes,result=proposal_changes(
            row,now=now,change_dates=True,schedule_q1=True,
            override_manual=True
        )
        self.assertIn("deadline_utc",changes)

    def test_auto_managed_deadline_tracks_new_q1(self):
        now=datetime.now(timezone.utc)
        event=fake_event("malaysia","GP OF MALAYSIA")
        old=(now+timedelta(days=15)).isoformat()
        row=fake_row(event,deadline=old,source="motogp_q1")
        updates,_=proposal_changes(
            row,now=now,change_dates=True,schedule_q1=True,
            override_manual=False
        )
        self.assertNotEqual(updates["deadline_utc"],old)

    def test_past_closed_and_missing_q1_never_close(self):
        now=datetime.now(timezone.utc)
        event=fake_event("malaysia","GP MALAYSIA",now-timedelta(days=10))
        row=fake_row(event,closed=True)
        changes,status=proposal_changes(
            row,now=now,change_dates=True,schedule_q1=True,
            override_manual=True
        )
        self.assertEqual(list(changes),["sessions"])
        self.assertIn("clôturé",status)
        row["closed"]=False
        changes,status=proposal_changes(
            row,now=now,change_dates=True,schedule_q1=True,
            override_manual=True
        )
        self.assertNotIn("deadline_utc",changes)
        self.assertEqual(status,"Q1 passée : clôture préservée")
        event2=fake_event("no-q1","GP MALAYSIA")
        event2["sessions"]=[x for x in event2["sessions"] if x["label"]!="Q1"]
        row=fake_row(event2)
        changes,status=proposal_changes(
            row,now=now,change_dates=False,schedule_q1=True,
            override_manual=True
        )
        self.assertEqual(status,"Q1 introuvable : clôture préservée")
        self.assertNotIn("deadline_utc",changes)

    def test_snapshot_digest_not_cookie_bulk_payload(self):
        event=fake_event("malaysia","GP MALAYSIA")
        row=fake_row(event)
        d1=batch_preview_digest(2026,[row])
        self.assertEqual(len(d1),64)
        row["event"]["sessions"][1]["utc"]=(datetime.now(timezone.utc)+timedelta(days=21)).isoformat()
        self.assertNotEqual(d1,batch_preview_digest(2026,[row]))


if __name__ == "__main__":
    unittest.main()
