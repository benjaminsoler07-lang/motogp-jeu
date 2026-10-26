"""Lecture seule du calendrier MotoGP Pulselive (API communautaire non garantie).

AUCUNE écriture et AUCUN changement de clôture ne peuvent provenir de ce module.
Seule une action explicite de l'administrateur applique une proposition vérifiée.
"""
from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import requests

API_BASE = "https://api.motogp.pulselive.com/motogp/v1"
PARIS = ZoneInfo("Europe/Paris")
UTC = timezone.utc

SESSION_NAMES = {
    "FP1": "FP1", "FP2": "FP2", "FP3": "FP3",
    "PR": "Practice", "P": "Practice", "PRACTICE": "Practice",
    "Q1": "Q1", "Q2": "Q2",
    "SPR": "Sprint", "SPRINT": "Sprint",
    "WUP": "Warm-up", "WARMUP": "Warm-up",
    "RAC": "Course", "RACE": "Course"
}


class MotoGPAPIError(Exception):
    """Erreur réseau ou réponse incompatible ; ne doit jamais modifier le calendrier."""


def parse_external_datetime(raw):
    if not isinstance(raw, str):
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return None
        return parsed.astimezone(UTC)
    except (ValueError, OverflowError):
        return None


def is_motogp_session(broadcast):
    category = broadcast.get("category")
    if isinstance(category, dict):
        name = str(category.get("name") or "").lower().replace("™", "").strip()
        acronym = str(category.get("acronym") or "").upper()
        return acronym == "MGP" or name == "motogp"
    return isinstance(category, str) and category.lower().replace("™", "").strip() == "motogp"


def extract_sessions(event):
    """N'accepte que les séances MotoGP de piste, avec heures absolues fiables."""
    sessions = []
    seen = set()
    for broadcast in event.get("broadcasts") or []:
        if not isinstance(broadcast, dict):
            continue
        if str(broadcast.get("type") or "").upper() != "SESSION":
            continue
        if not is_motogp_session(broadcast):
            continue
        code = str(broadcast.get("shortname") or "").upper().replace("-", "").strip()
        label = SESSION_NAMES.get(code)
        if not label:
            continue
        when = parse_external_datetime(broadcast.get("date_start"))
        if when is None:
            continue
        fingerprint = (label, when.isoformat())
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        sessions.append({"label": label, "utc": when.isoformat()})
    sessions.sort(key=lambda s: s["utc"])
    return sessions[:20]


def normalise_event(event, season_year):
    if not isinstance(event, dict):
        return None
    if str(event.get("kind") or "").upper() != "GP":
        return None
    if str(event.get("id") or "").strip() == "":
        return None
    season = event.get("season") or {}
    if isinstance(season, dict) and season.get("year"):
        try:
            if int(season["year"]) != season_year:
                return None
        except (TypeError, ValueError):
            return None
    sessions = extract_sessions(event)
    course = next((x for x in reversed(sessions) if x["label"] == "Course"), None)
    # Sans course horodatée, ne jamais déduire une date de GP en devinant.
    if not course:
        return None
    day = parse_external_datetime(course["utc"]).astimezone(PARIS).date()
    if abs(day.year - season_year) > 1:
        return None
    title = str(event.get("name") or event.get("shortname") or "").strip()[:140]
    if not title:
        return None
    return {
        "external_id": str(event["id"]),
        "name": title,
        "date": day.isoformat(),
        "sessions": sessions,
        "source": "MotoGP Pulselive (non officielle / non garantie)",
    }


def fetch_motogp_events(season_year):
    """Une URL fixe, aucune URL externe fournie par l'utilisateur (anti-SSRF)."""
    year = int(season_year)
    if not 2020 <= year <= 2100:
        raise MotoGPAPIError("Année de saison invalide.")
    try:
        response = requests.get(
            API_BASE + "/events",
            params={"seasonYear": year},
            timeout=(4, 12),
            allow_redirects=False,
            headers={"Accept": "application/json", "User-Agent": "PronoGP-Schedule/1.0"},
        )
        if response.status_code != 200:
            raise MotoGPAPIError(
                "API MotoGP indisponible (HTTP %s). Utilise la saisie manuelle." % response.status_code
            )
        if len(response.content) > 5_000_000:
            raise MotoGPAPIError("Réponse API trop volumineuse.")
        raw = response.json()
    except MotoGPAPIError:
        raise
    except (requests.RequestException, ValueError) as exc:
        raise MotoGPAPIError("Impossible de récupérer les horaires depuis MotoGP : %s" %
                            type(exc).__name__) from exc
    if isinstance(raw, dict):
        raw = raw.get("events") or raw.get("data")
    if not isinstance(raw, list):
        raise MotoGPAPIError("Format de calendrier MotoGP inattendu. Aucun horaire importé.")
    events = [normalise_event(e, year) for e in raw[:150]]
    events = [e for e in events if e is not None]
    if not events:
        raise MotoGPAPIError("Aucun GP MotoGP complet trouvé pour cette saison dans l'API.")
    events.sort(key=lambda e: (e["date"], e["name"]))
    return events
