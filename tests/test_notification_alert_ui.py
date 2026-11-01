"""Smoke tests for the non-blocking login alert, without PostgreSQL."""
import os
import unittest

os.environ.pop("DATABASE_URL", None)
from app import app
from flask import render_template, session


class NotificationAlertUITests(unittest.TestCase):
    def test_alert_flag_is_consumed_once_by_first_player_page(self):
        with app.test_request_context("/profile"):
            session["notification_login_alert"] = True
            first = render_template("base.html", name="TestPlayer")
            self.assertIn('data-on-login="1"', first)
            self.assertIn('data-notification-dialog hidden', first)
            self.assertIn("notification-alert.js", first)
            self.assertIn("notification-alert.css", first)
            self.assertIn("data-unread-count hidden", first)
            self.assertNotIn("notification_login_alert", session)
            second = render_template("base.html", name="TestPlayer")
            self.assertIn('data-on-login="0"', second)

    def test_guest_cannot_fetch_unread_messages_and_has_no_bell(self):
        visitor = app.test_client()
        anonymous = visitor.get("/notifications/unread-summary")
        self.assertEqual(anonymous.status_code, 302)
        self.assertTrue(anonymous.headers["Location"].endswith("/login"))
        with app.test_request_context("/login"):
            html = render_template("base.html", name=None)
            self.assertNotIn("data-notification-bell", html)
            self.assertNotIn('data-notification-dialog hidden', html)

    def test_alert_assets_are_served(self):
        visitor = app.test_client()
        js = visitor.get("/static/notification-alert.js")
        css = visitor.get("/static/notification-alert.css")
        self.assertEqual(js.status_code, 200)
        self.assertEqual(css.status_code, 200)
        self.assertIn("data-notification-unread-label", js.get_data(as_text=True))
        self.assertIn("prefers-reduced-motion", css.get_data(as_text=True))


if __name__ == "__main__":
    unittest.main()
