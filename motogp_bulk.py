"""Outils de synchronisation groupée : rapprochement strict et clôture Q1.

Les décisions et écritures appartiennent à l'administration Flask, jamais à l'API.
Ce module ne fait aucun appel réseau ou écriture en base.
"""
from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from datetime import datetime, timezone


# Les ID sont ceux des GP de PronoGP. Si aucun nom officiel n'est retrouvé
# sans ambiguïté, l'administrateur choisit manuellement dans la liste.
GP_ALIASES = {
    "thailande": ("thailand", "thai"),
    "bresil": ("brazil", "brasil"),
    "ameriques": ("united states", "americas", "america", "austin"),
    "qatar": ("qatar", "lusail"),
    "espagne": ("spain", "espana", "spanish", "jerez"),
    "france": ("france", "french", "le mans"),
    "catalogne": ("catalunya", "catalonia", "catalan", "barcelona"),
    "italie": ("italy", "italia", "italian", "mugello"),
    "hongrie": ("hungary", "hungarian", "balaton"),
    "republique-tcheque": ("czechia", "czech republic", "czech", "brno"),
    "pays-bas": ("netherlands", "dutch", "assen"),
    "allemagne": ("germany", "german", "sachsenring"),
    "grande-bretagne": ("great britain", "britain", "british", "united kingdom", "silverstone"),
    "aragon": ("aragon", "motorland"),
    "saint-marin": ("san marino", "rimini", "misano"),
    "autriche": ("austria", "austrian", "spielberg"),
    "japon": ("japan", "japanese", "motegi"),
    "indonesie": ("indonesia", "indonesian", "mandalika"),
    "australie": ("australia", "australian", "phillip island"),
    "malaisie": ("malaysia", "malaysian", "sepang"),
    "portugal": ("portugal", "portuguese", "portimao"),
    "valence": ("valencia", "valencian", "valenciana", "valence"),
}


def normalized_words(value):
    value = unicodedata.normalize("NFKD", str(value or "").casefold())
    value = "".join(char for char in value if not unicodedata.combining(char))
    return " ".join(re.findall(r"[a-z0-9]+", value))


def suggest_motogp_event(weekend_id, events):
    aliases = GP_ALIASES.get(str(weekend_id or "").strip().lower(), ())
    if not aliases:
        return None
    matches = []
    for event in events:
        title = " " + normalized_words(event.get("name")) + " "
        if any(" " + normalized_words(alias) + " " in title for alias in aliases):
            matches.append(event)
    return matches[0] if len(matches) == 1 else None


def q1_datetime(event):
    sessions = event.get("sessions") or []
    q1 = [s for s in sessions if s.get("label") == "Q1"]
    if len(q1) != 1:
        return None
    try:
        dt = datetime.fromisoformat(str(q1[0]["utc"]).replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc) if dt.tzinfo else None
    except (ValueError, TypeError, KeyError):
        return None


def batch_preview_digest(year, rows):
    """Empreinte des options, horaires API et révisions locales : stockée en session."""
    payload = {
        "year": int(year),
        "rows": [
            {
                "id": row["id"],
                "revision": row["revision"],
                "closed": bool(row["closed"]),
                "external_id": (row.get("event") or {}).get("external_id", ""),
                "date": (row.get("event") or {}).get("date", ""),
                "sessions": (row.get("event") or {}).get("sessions", []),
            }
            for row in sorted(rows, key=lambda x: x["id"])
        ]
    }
    data = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def proposal_changes(row, *, now, change_dates, schedule_q1, override_manual):
    """Calcule les changements d'un seul GP ; ne ferme jamais un GP déjà fermé."""
    event = row.get("event")
    if not event or not event.get("sessions"):
        raise ValueError("Grand Prix officiel sans séances MotoGP.")
    updates = {"sessions": event["sessions"]}
    q1_result = "non demandé"

    if change_dates and not row["closed"]:
        if str(event.get("date", ""))[:4] != str(row["year"]):
            raise ValueError("Date de course hors de la saison.")
        updates["date"] = event["date"]

    if schedule_q1:
        q1 = q1_datetime(event)
        old_deadline = row.get("current_deadline") or ""
        source = row.get("deadline_source") or ""
        if row["closed"]:
            q1_result = "GP clôturé : clôture préservée"
        elif q1 is None:
            q1_result = "Q1 introuvable : clôture préservée"
        elif q1 <= now:
            q1_result = "Q1 passée : clôture préservée"
        elif old_deadline and source != "motogp_q1" and not override_manual:
            q1_result = "Clôture personnalisée préservée"
        else:
            updates["deadline_utc"] = q1.isoformat()
            updates["deadline_source"] = "motogp_q1"
            q1_result = "Clôture Q1 programmée"
    return updates, q1_result
