"""Regression tests for moving light points tracking the actual background circuit."""
import os
import unittest
import xml.etree.ElementTree as ET

from flask import render_template

os.environ.pop("DATABASE_URL", None)
from app import app


class RacingTrackRidersTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()

    def test_three_moving_points_follow_the_exact_existing_track(self):
        background_response = self.client.get("/static/racing-track-v4.svg")
        rider_response = self.client.get("/static/racing-track-riders-v1.svg")
        self.assertEqual(background_response.status_code, 200)
        self.assertEqual(rider_response.status_code, 200)

        background = ET.fromstring(background_response.data)
        riders = ET.fromstring(rider_response.data)
        self.assertEqual(background.attrib["viewBox"], riders.attrib["viewBox"])

        original_path = next(
            node for node in background.iter()
            if node.tag.endswith("}path") and node.attrib.get("d", "").startswith("M115 329C-10")
        )
        animated_path = next(
            node for node in riders.iter() if node.attrib.get("id") == "gp-rider-lap"
        )
        self.assertEqual(animated_path.attrib["d"], original_path.attrib["d"])

        track_transform = "translate(800 105) rotate(-7 250 220)"
        self.assertTrue(any(node.attrib.get("transform") == track_transform for node in background.iter()))
        self.assertTrue(any(node.attrib.get("transform") == track_transform for node in riders.iter()))

        moving = [node for node in riders.iter() if node.tag.endswith("}animateMotion")]
        self.assertEqual(len(moving), 3)
        for node in moving:
            self.assertEqual(node.attrib["repeatCount"], "indefinite")
            self.assertEqual(node.attrib["rotate"], "auto")
            path_reference = next(child for child in node if child.tag.endswith("}mpath"))
            self.assertEqual(path_reference.attrib["href"], "#gp-rider-lap")

        svg_text = rider_response.get_data(as_text=True)
        for colour in ("#31deff", "#ff3f96", "#ffd653"):
            self.assertIn(colour, svg_text)
        self.assertIn("prefers-reduced-motion: reduce", svg_text)

    def test_overlay_is_background_only_and_responsive(self):
        with app.test_request_context("/"):
            html = render_template("base.html", name="Alice")
        self.assertIn('class="gp-track-riders"', html)
        self.assertIn('alt="" aria-hidden="true" width="1440" height="720"', html)
        self.assertIn("racing-track-riders-v1.svg", html)
        self.assertIn("v=track-riders-v1", html)
        css = self.client.get("/static/racing-v4.css").get_data(as_text=True)
        self.assertIn("main > .gp-track-riders", css)
        self.assertIn("top:15px", css)
        self.assertIn("right:-1vw", css)
        self.assertIn("width:min(1350px,135vw)", css)
        self.assertIn("racing-track-riders-v1.svg", css)
        self.assertIn("pointer-events:none", css)
        self.assertIn("@media (prefers-reduced-motion: reduce)", css)


if __name__ == "__main__":
    unittest.main()
