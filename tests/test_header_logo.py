import io
import os
import unittest
from PIL import Image
from flask import render_template

os.environ.pop("DATABASE_URL", None)

from app import app


class HeaderLogoTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()

    def test_approved_logo_is_served_as_compact_webp(self):
        response = self.client.get("/static/pronogp-header-logo.webp")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.data.startswith(b"RIFF"))
        self.assertIn(b"WEBP", response.data[:16])
        self.assertLess(len(response.data), 20000)
        image = Image.open(io.BytesIO(response.data))
        self.assertEqual(image.size, (420, 97))

    def test_logo_replaces_old_header_brand_without_duplicate_typography(self):
        with app.test_request_context("/"):
            markup = render_template("base.html", name="Alice")
        self.assertIn("gp-header-brand-art", markup)
        self.assertIn("gp-header-neon-logo", markup)
        self.assertIn("pronogp-header-logo.webp", markup)
        self.assertIn('aria-label="Accueil PronoGP"', markup)
        self.assertNotIn('gp-header-flag" aria-hidden="true"', markup)
        self.assertIn("header-logo.css", markup)
        for label in ("Classement", "Résultats GP", "Championnat", "Navigation mobile"):
            self.assertIn(label, markup)

    def test_desktop_mobile_styles_keep_navigation_legible(self):
        response = self.client.get("/static/header-logo.css")
        self.assertEqual(response.status_code, 200)
        css = response.get_data(as_text=True)
        for rule in ("@media(max-width:720px)", "@media(max-width:385px)",
                     "gp-header-brand-art", "gp-header-tools", "gp-header-nav",
                     "mix-blend-mode:screen", "#ffd653", "#ff3f96"):
            self.assertIn(rule, css)


if __name__ == "__main__":
    unittest.main()
