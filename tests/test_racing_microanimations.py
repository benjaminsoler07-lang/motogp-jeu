"""Visual-only racing motion regression checks, including the unchanged scoring UI."""
import os
import shutil
import subprocess
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

from flask import render_template

os.environ.pop("DATABASE_URL", None)
from app import app

ROOT = Path(__file__).resolve().parents[1]


class RacingMicroanimationsTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()

    def get_static(self, name):
        resp = self.client.get("/static/" + name)
        self.assertEqual(resp.status_code, 200)
        return resp.get_data(as_text=True)

    def test_shared_assets_are_loaded_on_every_page(self):
        with app.test_request_context("/"):
            html = render_template("base.html", name="Alice")
        self.assertIn("racing-microanimations.css?v=race-micro-v1", html)
        self.assertIn("racing-microanimations.js?v=race-micro-v1", html)
        self.assertIn("notification-alert.js", html)
        self.assertIn("header-logo.css", html)

    def test_sector_pulses_are_anchored_to_circuit_rider_timing(self):
        art = self.get_static("racing-track-animated-v5.svg")
        svg = ET.fromstring(art)
        track = next(e for e in svg.iter() if e.attrib.get("id") == "gp-track-centerline")
        self.assertTrue(track.attrib["d"].startswith("M115 329"))
        bikes = [e for e in svg.iter() if e.attrib.get("class") == "gp-track-bike"]
        sectors = [e for e in svg.iter() if e.attrib.get("class") == "gp-sector-flash"]
        self.assertEqual(len(bikes), 3)
        self.assertEqual(len(sectors), 9)
        durations = {c.attrib["dur"] for sector in sectors for c in sector if c.tag.endswith("}animate")}
        self.assertEqual(durations, {"19s", "20s", "21s"})
        positions = {(float(s.attrib["cx"]), float(s.attrib["cy"])) for s in sectors}
        self.assertEqual(positions, {(239.0,90.0),(469.0,80.0),(549.0,409.0)})
        self.assertIn(".gp-sector-flash { display: none; }", art)

    def test_glossy_buttons_and_accessible_nonblocking_motion(self):
        css = self.get_static("racing-microanimations.css")
        for rule in ("gp-racing-gloss", "gp-podium-rise", "gp-prono-red",
                     "gp-prono-green", "gp-saved-confirm", "gp-bell-halo",
                     "gp-race-progress", "prefers-reduced-motion:reduce"):
            self.assertIn(rule, css)
        self.assertIn("pointer-events:none", css)
        self.assertNotIn("animation:gp-racing-gloss", self.get_static("racing-v4.css"))

    def test_existing_countdowns_are_preserved_with_real_deadlines(self):
        js = self.get_static("v2-countdown.js")
        race_center = (ROOT / "templates/_race_center.html").read_text()
        self.assertIn('Date.parse(node.dataset.countdown)', js)
        self.assertIn('secs / 604800', js)
        self.assertIn("gp-race-is-soon", js)
        self.assertIn("gp-race-is-imminent", js)
        self.assertIn('data-countdown="{{ slot.iso }}"', race_center)
        self.assertIn('data-countdown="{{ rc.deadline_iso }}"', race_center)
        self.assertIn('data-countdown="{{ rc.session_iso }}"', race_center)

    def test_form_success_is_based_on_server_flash_not_button_press(self):
        js = self.get_static("racing-microanimations.js")
        form = (ROOT / "templates/pronos.html").read_text()
        self.assertIn("form.prono-entry-form", js)
        self.assertIn("main .flash", js)
        self.assertIn("Pronostic enregistré", js)
        self.assertIn("gp-prono-saved-confirmation", js)
        self.assertIn("gp-prono-lights", js)
        self.assertIn('method="post" class="card prono-entry-form"', form)
        self.assertNotIn("fetch(", js)

    def test_score_counters_and_podium_do_not_change_scoring(self):
        js = self.get_static("racing-microanimations.js")
        self.assertIn("data-gp-counter", js)
        self.assertIn("node.textContent = valueText", js)
        self.assertIn("IntersectionObserver", js)
        for template in ("index.html", "profile.html", "classement_general.html",
                         "results_by_race.html", "classement.html"):
            self.assertIn("data-gp-counter", (ROOT / "templates" / template).read_text(), template)
        self.assertIn("podium-card", (ROOT / "templates/results_by_race.html").read_text())

    def test_bell_glow_triggers_on_new_unread_only(self):
        alert = self.get_static("notification-alert.js")
        self.assertIn("afterLogin || newForThisTab", alert)
        self.assertIn("gp-new-notification", alert)
        self.assertIn("sessionStorage", alert)

    def test_javascript_syntax_when_node_available(self):
        node = shutil.which("node")
        if not node:
            self.skipTest("Node unavailable in this environment")
        for path in ("racing-microanimations.js", "v2-countdown.js", "notification-alert.js"):
            result = subprocess.run([node, "--check", str(ROOT / "static" / path)],
                                    capture_output=True, text=True, timeout=15)
            self.assertEqual(result.returncode, 0, path + ": " + result.stderr)


if __name__ == "__main__":
    unittest.main()
