import os
import unittest
from datetime import datetime, timezone, timedelta

os.environ.pop("DATABASE_URL", None)
from app import app
from v2_features import deadline_from_weekend, parse_paris_local, race_center


class V2PagesTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()

    def test_prediction_form_has_colored_sections_and_native_dropdowns(self):
        from flask import render_template
        from pathlib import Path

        weekend = {
            "id": "ci-demo", "label": "GP CI", "deadline_utc": "",
            "bonus_questions": [
                {"id": "yn", "label": "Safety car ?", "type": "yes_no"},
                {"id": "rider", "label": "Pilote bonus", "type": "rider"},
            ],
        }
        with app.test_request_context("/w/ci-demo/pronos"):
            html = render_template(
                "pronos.html", w=weekend, name="Joueur CI", closed=False, my={},
                riders=["#1 Pilote CI", "#2 Pilote B"], all_teams=[],
                official_teams=[], satellite_teams=[],
            )

        for section in ("is-qualifying", "is-sprint", "is-grand-prix"):
            self.assertIn(section, html)
        self.assertIn("prono-entry-form", html)
        self.assertIn("pronos-color-2", html)
        self.assertNotIn("v2-rider-search.js", html)
        self.assertNotIn('type="search"', html)
        self.assertIn("removeLegacyRiderSearch", html)
        self.assertNotIn("N° ou nom du pilote", html)
        for field in ("pole", "q1_1", "q1_2", "sprint_p1", "sprint_p2",
                      "sprint_p3", "gp_p1", "gp_p2", "gp_p3"):
            self.assertIn(f'name="{field}"', html)
        self.assertEqual(html.count("— Choisir un pilote —"), 10)
        self.assertIn("— Choisir une réponse —", html)
        self.assertIn("disableDuplicates", html)

        css = Path(app.static_folder, "racing-v4.css").read_text(encoding="utf-8")
        for gradient in ("#196f8d", "#66459a", "#983e58", "#86601f", "#236e9e", "#23785a"):
            self.assertIn(gradient, css)
        deprecated = Path(app.static_folder, "v2-rider-search.js").read_text(encoding="utf-8")
        self.assertNotIn("createElement('input')", deprecated)

    def test_prediction_form_disables_browser_cache(self):
        from unittest.mock import patch
        from flask import Response

        with self.client.session_transaction() as state:
            state["is_admin"] = True
        from app import get_sorted_weekends
        weekends = get_sorted_weekends()
        self.assertTrue(weekends)
        weekend_id = weekends[0]["id"]
        with patch("app.is_weekend_closed", return_value=False):
            response = self.client.get(f"/w/{weekend_id}/pronos")
        self.assertEqual(response.status_code, 200)
        self.assertIn("no-store", response.headers.get("Cache-Control", ""))
        self.assertEqual(response.headers.get("Vary"), "Cookie")
        self.assertNotIn("v2-rider-search.js", response.get_data(as_text=True))

    def test_vercel_import_uses_writable_tmp_and_preserves_calendar(self):
        import subprocess
        import sys
        env = dict(os.environ)
        env.pop("DATABASE_URL", None)
        env["VERCEL"] = "1"
        code = (
            "import os, app; "
            "assert app.DATA_DIR.startswith('/tmp/'); "
            "assert os.path.isdir(app.RESULTS_DIR); "
            "assert os.path.isdir(app.PRONOS_DIR); "
            "assert os.path.isfile(app.WEEKENDS_FILE); "
            "assert app.load_json(app.WEEKENDS_FILE, {}).get('weekends'); "
            "assert app.app.test_client().get('/healthz').status_code == 200"
        )
        result = subprocess.run([sys.executable, "-c", code], env=env,
                                text=True, capture_output=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_private_pages_redirect_anonymous_to_login(self):
        from urllib.parse import urlsplit
        for url in ("/", "/classement", "/results_by_race",
                    "/championnat/public", "/me", "/profile", "/avatar/1"):
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 302)
                self.assertEqual(urlsplit(response.headers["Location"]).path, "/login")

    def test_login_and_technical_pages_remain_available(self):
        for url in ("/login", "/healthz",
                    "/manifest.webmanifest", "/service-worker.js"):
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 200, response.get_data(as_text=True)[:400])
        # L'administration doit rester accessible sans session, mais renvoie
        # 500 dans le job CI qui ne configure volontairement aucun admin.
        from app import admin_enabled
        self.assertEqual(self.client.get("/admin/login").status_code,
                         200 if admin_enabled() else 500)

    def test_login_page_hides_private_menu_for_guest(self):
        html = self.client.get("/login").get_data(as_text=True)
        self.assertNotIn('class="header-nav"', html)
        self.assertIn("Connexion", html)

    def test_admin_can_browse_with_admin_session_only(self):
        with self.client.session_transaction() as state:
            state["is_admin"] = True
        for url in ("/", "/classement", "/results_by_race"):
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 200, response.get_data(as_text=True)[:400])

    def test_template_smoke_race_center(self):
        with app.test_request_context("/"):
            from flask import render_template
            html = render_template("_race_center.html", rc={
                "id":"qatar", "name":"GP du Qatar", "deadline_iso":"2026-10-25T12:00:00+00:00",
                "session_label":"Q1", "session_iso":"2026-10-25T12:30:00+00:00"
            })
            self.assertIn("Clôture des pronostics", html)
            self.assertIn("GP du Qatar", html)
            html=render_template("profile.html", name="Alex", person={"id":1,"pseudo":"Alex"},
               csrf="test", points=12.5, rank=2, gp_count=1,
               last_result={"name":"GP Test","points":12.5}, season_year=2026,
               # Même si un appelant injecte accidentellement le calendrier,
               # le template profil ne doit jamais l'afficher.
               weekends=[{"id":"futur-ci", "label":"GP du futur à cacher", "date":"2026-11-01"}],
               next_gp={"name":"Race Center caché", "deadline_iso":"2026-10-31T15:00:00+00:00"})
            self.assertIn("Mon profil", html)
            self.assertIn("Enregistrer ma photo", html)
            self.assertIn("Changer mon mot de passe", html)
            self.assertIn('name="current_password"', html)
            self.assertIn('name="new_password"', html)
            self.assertIn('name="confirm_password"', html)
            self.assertIn("MES POINTS", html)
            self.assertIn("MON CLASSEMENT", html)
            self.assertIn("Mon bilan", html)
            self.assertIn("GP CLASSÉS", html)
            self.assertIn("DERNIER RÉSULTAT", html)
            self.assertIn("GP Test", html)
            self.assertNotIn("Mes pronostics", html)
            self.assertNotIn("v2-event-grid", html)
            self.assertNotIn("GP du futur à cacher", html)
            self.assertNotIn("Race Center caché", html)
            self.assertNotIn("Clôture des pronostics", html)
            self.assertNotIn("Mon espace", html)
            # Les statistiques doivent être dans la page et non dans <title>.
            self.assertNotIn("<h2>", html.split("</title>")[0])
            self.assertIn("Mon bilan", html.split("</title>")[1])

    def test_home_selection_excludes_past_cancelled_and_duplicate_gp(self):
        from datetime import date
        from app import home_upcoming_weekends

        today = date(2026, 10, 26)
        fixtures = [
            {"id":"past", "label":"GP passé", "date_obj":date(2026,10,10)},
            {"id":"cancelled", "label":"GP annulé", "date_obj":date(2026,11,3),"cancelled":True},
            {"id":"race", "label":"GP actuel", "date_obj":date(2026,10,30)},
            {"id":"future1", "label":"GP Portugal", "date_obj":date(2026,11,8)},
            {"id":"future2", "label":"GP Valence", "date_obj":date(2026,11,15)},
            {"id":"future3", "label":"GP en décembre", "date_obj":date(2026,12,1)},
            {"id":"future4", "label":"GP janvier", "date_obj":date(2027,1,12)},
            {"id":"no_date", "label":"Date inconnue", "date_obj":None},
        ]
        selected = home_upcoming_weekends(fixtures,today=today,exclude_id="race",limit=3)
        self.assertEqual([w["id"] for w in selected],["future1","future2","future3"])

    def test_home_race_first_and_no_past_gp_on_page(self):
        from datetime import date, timedelta
        from unittest.mock import patch

        today = date.today()
        year = today.year
        make_gp = lambda i, label, delta: {
            "id":i, "label":label, "date":(today+timedelta(days=delta)).isoformat(),
            "date_obj":today+timedelta(days=delta), "season_year":year,
            "cancelled":False, "sessions":[]
        }
        fixtures = [
            make_gp("past","GP passé qui ne doit pas apparaître",-14),
            make_gp("race","GP principal",7),
            make_gp("future1","GP suivant 1",14),
            make_gp("future2","GP suivant 2",21),
            make_gp("future3","GP suivant 3",28),
            make_gp("future4","GP suivant 4 caché",35),
        ]
        with self.client.session_transaction() as st:
            st["is_admin"]=True
        with (patch("app.get_sorted_weekends",return_value=fixtures),
              patch("app.is_weekend_closed",return_value=False),
              patch("app.is_pronos_public",return_value=False)):
            response = self.client.get(f"/?season={year}")
        self.assertEqual(response.status_code,200)
        html = response.get_data(as_text=True)
        self.assertIn("Le prochain Grand Prix",html)
        self.assertIn("GP principal",html)
        self.assertEqual(html.count("GP principal"),2) # titre + aria-label
        for label in ("GP suivant 1","GP suivant 2","GP suivant 3"):
            self.assertIn(label,html)
        self.assertNotIn("GP passé qui ne doit pas apparaître",html)
        self.assertNotIn("GP suivant 4 caché",html)
        self.assertNotIn("Une saison. Des Grands Prix",html)
        self.assertNotIn("Calendrier MotoGP",html)
        self.assertIn("Faire mon pronostic",html)
        self.assertIn("Clôture des pronostics",html)
        self.assertIn("Classement général",html)
        self.assertIn("Prono championnat",html)
        self.assertIn("Résultats par GP",html)

    def test_race_center_closed_or_open_action_does_not_mislead(self):
        from flask import render_template
        with app.test_request_context("/"):
            rc = {
                "id":"test","name":"GP Test","closed":True,"public":False,"played":True,
                "deadline_iso":None,"session_iso":None,"session_label":None
            }
            html=render_template("_race_center.html",rc=rc)
            self.assertIn("Pronostics fermés",html)
            self.assertNotIn("Faire mon pronostic",html)
            self.assertIn("Ton pronostic est enregistré",html)
            rc["public"]=True
            html=render_template("_race_center.html",rc=rc)
            self.assertIn("Voir les pronostics publics",html)
            rc["closed"]=False
            rc["public"]=False
            html=render_template("_race_center.html",rc=rc)
            self.assertIn("Modifier mon pronostic",html)

    def test_results_gp_picker_search_navigation_and_season_links(self):
        from datetime import date
        from flask import render_template

        weekends = [
            {"id": "qatar-2026", "label": "GP du Qatar",
             "season_year": 2026, "date_obj": date(2026, 4, 12), "cancelled": False},
            {"id": "italie-2026", "label": "GP d'Italie",
             "season_year": 2026, "date_obj": date(2026, 5, 31), "cancelled": False},
            {"id": "france-2026", "label": "GP de France",
             "season_year": 2026, "date_obj": date(2026, 6, 14), "cancelled": False},
        ]
        with app.test_request_context("/results_by_race?season=2026"):
            html = render_template("results_by_race.html", name="Alex",
                                   weekends=weekends, selected=weekends[1],
                                   results=None, rows=[], notice=None)

        self.assertIn('id="results-gp-search"', html)
        self.assertIn('data-gp-label="GP du Qatar"', html)
        self.assertIn('aria-current="page"', html)
        self.assertNotIn("GP précédent", html)
        self.assertNotIn("GP suivant", html)
        self.assertIn("GP du Qatar", html)
        self.assertIn("GP de France", html)
        self.assertIn("season=2026", html)
        self.assertNotIn('name="gp" onchange=', html)
        # Drapeaux affichés sans supprimer les noms ni casser la recherche.
        for flag in ("🇶🇦", "🇮🇹", "🇫🇷"):
            self.assertIn(f'<span class="results-gp-flag" aria-hidden="true">{flag}</span>', html)
        self.assertIn('data-gp-label="GP du Qatar"', html)

    def test_results_gp_flags_regions_and_unknown_gp(self):
        from flask import render_template
        weekends = [
            {"id": "saint-marin-2026", "label": "GP de Saint-Marin"},
            {"id": "aragon-2026", "label": "GP d'Aragon"},
            {"id": "gp-personnalise", "label": "Course bonus"},
        ]
        with app.test_request_context("/results_by_race?season=2026"):
            html = render_template("results_by_race.html", name="Alex",
                                   weekends=weekends, selected=weekends[0],
                                   results=None, rows=[], notice=None)
        self.assertIn("🇸🇲", html)
        self.assertIn("🇪🇸", html)
        self.assertIn("Course bonus", html)
        self.assertEqual(html.count("results-gp-flag"), 3)

    def test_results_gp_direct_link_keeps_archived_season(self):
        from datetime import date
        from unittest.mock import patch

        old = {"id": "italie-2025", "label": "GP Italie 2025",
               "season_year": 2025, "date_obj": date(2025, 6, 15), "cancelled": True}
        current = {"id": "france-2026", "label": "GP France 2026",
                   "season_year": 2026, "date_obj": date(2026, 6, 14), "cancelled": False}
        with self.client.session_transaction() as state:
            state["is_admin"] = True
        with (patch("app.get_weekend", return_value=old),
              patch("app.get_sorted_weekends", return_value=[old, current])):
            response = self.client.get("/results_by_race?gp=italie-2025")
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn('data-gp-label="GP Italie 2025"', html)
        self.assertNotIn('data-gp-label="GP France 2026"', html)

    def test_local_time_paris_in_utc(self):
        utc=parse_paris_local("2026-10-25T11:30")
        self.assertEqual(utc.utcoffset(),timedelta())
        self.assertEqual(utc.hour,10)
        self.assertEqual(deadline_from_weekend({"deadline_utc":utc.isoformat()}),utc)
        with self.assertRaises(ValueError):
            parse_paris_local("2026-03-29T02:30")

    def test_no_automatic_close_without_explicit_deadline(self):
        self.assertIsNone(deadline_from_weekend({"date":"2026-10-25"}))
        self.assertIsNone(race_center([{"id":"a","label":"Test","date":"2026-10-25","sessions":[]}]))

if __name__ == "__main__":
    unittest.main()
