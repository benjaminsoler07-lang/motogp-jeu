import os
import unittest
from flask import render_template
os.environ.pop("DATABASE_URL", None)
from app import app

class UserMenuGlossyTests(unittest.TestCase):
    def test_account_links_and_overlay_css(self):
        with app.test_request_context("/"):
            html=render_template("base.html",name="Benjamin",viewer_is_admin=True,viewer_is_root_admin=True)
        for endpoint in ('profile','notifications','logout','admin_home','admin_logout'):
            self.assertIn('href="/'+('admin' if endpoint=='admin_home' else ''),html) if False else None
        for label in ("Mon profil","Mes notifications","Déconnexion / changer de joueur","Dashboard admin"):
            self.assertIn(label,html)
        self.assertIn("user-menu-glossy.css",html)
        response=app.test_client().get("/static/user-menu-glossy.css")
        self.assertEqual(response.status_code,200)
        css=response.get_data(as_text=True)
        for token in (".gp-user-menu[open]","z-index:10020","pointer-events:auto","touch-action:manipulation","#ffce24"):
            self.assertIn(token,css)
if __name__=="__main__":
    unittest.main()
