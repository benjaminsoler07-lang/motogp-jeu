"""Keep moving MotoGP lights on the exact path used to draw the circuit."""
import os
import unittest
import xml.etree.ElementTree as ET
from flask import render_template

os.environ.pop("DATABASE_URL", None)
from app import app


class RacingTrackRidersTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()

    def test_moving_bikes_share_visible_stroke_and_parent_transform(self):
        original_response = self.client.get("/static/racing-track-v4.svg")
        animated_response = self.client.get("/static/racing-track-animated-v5.svg")
        self.assertEqual(original_response.status_code, 200)
        self.assertEqual(animated_response.status_code, 200)
        original = ET.fromstring(original_response.data)
        animated = ET.fromstring(animated_response.data)
        self.assertEqual(original.attrib["viewBox"], animated.attrib["viewBox"])

        def find_track(root):
            for node in root.iter():
                if node.attrib.get("stroke") == "url(#trackstroke)":
                    return node
            self.fail("Visible neon track path not found")

        source_line = find_track(original)
        animated_line = find_track(animated)
        self.assertEqual(source_line.attrib["d"], animated_line.attrib["d"])
        self.assertEqual(animated_line.attrib["id"], "gp-track-centerline")

        ns = "{http://www.w3.org/2000/svg}"
        group = next(
            node for node in animated.iter()
            if node.attrib.get("transform") == "translate(800 105) rotate(-7 250 220)"
        )
        self.assertIn(animated_line, list(group))
        riders = [child for child in group if child.attrib.get("class") == "gp-track-bike"]
        self.assertEqual(len(riders), 3)
        self.assertEqual([child.attrib["color"] for child in riders],
                         ["#31deff", "#ff3f96", "#ffd653"])
        for rider in riders:
            animations = [node for node in rider if node.tag == ns + "animateMotion"]
            self.assertEqual(len(animations), 1)
            animation = animations[0]
            self.assertEqual(animation.attrib["repeatCount"], "indefinite")
            self.assertEqual(animation.attrib["rotate"], "auto")
            link = animation.find(ns + "mpath")
            self.assertIsNotNone(link)
            self.assertEqual(link.attrib["href"], "#gp-track-centerline")
        self.assertIn("prefers-reduced-motion: reduce",
                      animated_response.get_data(as_text=True))

    def test_all_background_layouts_use_single_artwork_not_overlay(self):
        with app.test_request_context("/"):
            html = render_template("base.html", name="Alice")
        self.assertNotIn('class="gp-track-riders"', html)
        self.assertNotIn("racing-track-riders-v1.svg", html)
        self.assertIn("pronos-color-2-track-aligned-v5", html)

        css = self.client.get("/static/racing-v4.css").get_data(as_text=True)
        self.assertEqual(css.count("url('/static/racing-track-animated-v5.svg')"), 4)
        self.assertNotIn("main > .gp-track-riders", css)
        self.assertIn(".v3-racecenter", css)
        self.assertIn("main::before", css)
        self.assertIn("racing-track-v4.svg", css)  # original preserved
        self.assertIn("@media(max-width:720px)", css)


if __name__ == "__main__":
    unittest.main()
