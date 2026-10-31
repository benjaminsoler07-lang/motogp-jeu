"""Analyses en lecture seule pour les pronostics publics et le championnat.

Aucun accès direct à la base ni modification de barème. Ces fonctions reçoivent
uniquement des pronostics dont la publication a été autorisée par la route Flask.
"""
from collections import Counter


def _display(value):
    return str(value).strip() if value is not None and str(value).strip() else "—"


def _key(value):
    return _display(value).casefold()


def _distribution(pronos, field, title, limit=4):
    choices = [_display((row.get("p") or {}).get(field)) for row in pronos]
    counted = Counter(_key(choice) for choice in choices if choice != "—")
    labels = {}
    for choice in choices:
        if choice != "—":
            labels.setdefault(_key(choice), choice)
    if not counted:
        return None
    ordered = sorted(counted.items(), key=lambda item: (-item[1], labels[item[0]].casefold()))
    values = [{"name": labels[key], "votes": amount,
               "percent": round(100 * amount / len(pronos))}
              for key, amount in ordered[:limit]]
    remainder = sum(n for _, n in ordered[limit:])
    if remainder:
        values.append({"name": "Autres choix", "votes": remainder,
                       "percent": round(100 * remainder / len(pronos))})
    return {"title": title, "count": sum(counted.values()),
            "leader": labels[ordered[0][0]], "votes": ordered[0][1],
            "choices": len(ordered), "bars": values}


def build_public_insights(pronos, weekend):
    """Tableau transposé (joueurs en colonnes) et analyse pré-course."""
    players = [{"name": _display(p.get("player")), "updated_at": p.get("updated_at")}
               for p in pronos]
    field_specs = [
        ("Qualifications", "Pole", "pole"),
        ("Qualifications", "Q1 · pilote 1", "q1_1"),
        ("Qualifications", "Q1 · pilote 2", "q1_2"),
        ("Sprint", "Sprint · P1", "sprint_p1"),
        ("Sprint", "Sprint · P2", "sprint_p2"),
        ("Sprint", "Sprint · P3", "sprint_p3"),
        ("Grand Prix", "GP · P1", "gp_p1"),
        ("Grand Prix", "GP · P2", "gp_p2"),
        ("Grand Prix", "GP · P3", "gp_p3"),
    ]
    bonus_specs = [
        ("Questions bonus", str(b.get("label") or b["id"]), str(b["id"]))
        for b in (weekend.get("bonus_questions") or []) if b.get("id")
    ]

    matrix = []
    for group, label, field in field_specs + bonus_specs:
        choices = [_display(((p.get("p") or {}).get("bonus") or {}).get(field)
                    if group == "Questions bonus" else (p.get("p") or {}).get(field))
                   for p in pronos]
        counts = Counter(_key(c) for c in choices if c != "—")
        top = max(counts.values()) if counts else 0
        cells = [{"value": value,
                  "popular": value != "—" and top >= 2 and counts[_key(value)] == top}
                 for value in choices]
        matrix.append({"group": group, "label": label, "cells": cells,
                       "unique": len(counts)})

    focus = [
        _distribution(pronos, "pole", "La pole"),
        _distribution(pronos, "sprint_p1", "Vainqueur du Sprint"),
        _distribution(pronos, "gp_p1", "Vainqueur du GP"),
    ]
    focus = [d for d in focus if d]
    comments = []
    n = len(pronos)
    if n:
        comments.append({"icon": "👥", "title": "La grille est en place",
                         "text": f"{n} joueur(s) ont un pronostic publié pour ce Grand Prix."})
    for d in focus:
        if d["votes"] == n:
            detail = f"Unanimité : les {n} joueurs ont choisi {d['leader']} pour « {d['title']} »."
        elif d["votes"] > 1:
            detail = (f"{d['leader']} recueille {d['votes']} pronostic(s) sur {n} "
                      f"pour « {d['title']} ».")
        else:
            detail = f"Aucun favori ne se détache pour « {d['title']} » : {d['choices']} choix différents."
        comments.append({"icon": "🎯", "title": d["title"], "text": detail})

    if n > 1:
        rows = [r for r in matrix if r["group"] != "Questions bonus"
                and r["unique"] > 1]
        if rows:
            divided = max(rows, key=lambda r: r["unique"])
            comments.append({"icon": "⚡", "title": "Le choix qui divise",
                             "text": (f"Le poste « {divided['label']} » présente "
                                      f"{divided['unique']} choix différents parmi les joueurs.")})
        complete_gp = []
        for p in pronos:
            fields = [((p.get("p") or {}).get(f"gp_p{i}")) for i in (1, 2, 3)]
            if all(_display(v) != "—" for v in fields):
                complete_gp.append(tuple(_key(v) for v in fields))
        if complete_gp:
            different = len(set(complete_gp))
            comments.append({"icon": "🏁", "title": "Podiums imaginés",
                             "text": f"{different} podium(s) GP distinct(s) proposés parmi {len(complete_gp)} pronostics complets."})
    return {"players": players, "matrix": matrix, "distributions": focus,
            "comments": comments, "count": n}


def build_championship_insights(standings, rounds):
    """Analyse du classement en fonction des points déjà calculés par l'application."""
    if not standings or not rounds:
        return {"cards": [], "comments": [], "bars": [], "recent": []}
    leader = standings[0]
    n = len(standings)
    gap = round(leader["total"] - standings[1]["total"], 2) if n > 1 else None
    best = max((r for r in standings if r["gps"] > 0),
               key=lambda r: (r["total"] / r["gps"], r["total"]), default=None)
    cards = [
        {"label": "GP comptabilisés", "value": len(rounds), "detail": "Résultats enregistrés"},
        {"label": "Joueurs classés", "value": n, "detail": "Au moins un GP"},
        {"label": "Leader", "value": leader["player"], "detail": f"{leader['total']:g} points"},
        {"label": "Écart en tête", "value": f"{gap:g} pt(s)" if gap is not None else "—",
         "detail": "1er / 2e du classement"},
    ]
    comments = []
    if gap is not None and gap == 0:
        comments.append({"icon": "🔥", "title": "Championnat indécis",
                         "text": f"{leader['player']} partage la première place avec {standings[1]['player']} ({leader['total']:g} points)."})
    elif gap is not None:
        comments.append({"icon": "🏆", "title": "Course au titre",
                         "text": (f"{leader['player']} mène avec {leader['total']:g} points, "
                                  f"{gap:g} point(s) devant {standings[1]['player']}.")})
    else:
        comments.append({"icon": "🏆", "title": "Course au titre",
                         "text": f"{leader['player']} occupe la première place avec {leader['total']:g} points."})
    if best:
        comments.append({"icon": "📈", "title": "Meilleure moyenne",
                         "text": (f"{best['player']} marque en moyenne "
                                  f"{best['total'] / best['gps']:.2f} points par GP joué "
                                  f"({best['gps']} GP).")})
    if len(rounds) > 1:
        latest = rounds[-1]
        scores = latest.get("scores") or {}
        if scores:
            top = max(scores.values())
            victors = [name for name, value in scores.items() if value == top]
            comments.append({"icon": "⚡", "title": "Dernière manche",
                             "text": (f"{', '.join(victors)} ont réalisé le meilleur score "
                                      f"au {latest['label']} avec {top:g} points.")})
    max_score = max(float(row["total"]) for row in standings) or 1.0
    bars = [{"name": r["player"], "total": r["total"],
             "percent": round(100 * r["total"] / max_score), "gps": r["gps"]}
            for r in standings[:5]]
    recent = []
    for round_data in rounds[-5:]:
        scores = round_data.get("scores") or {}
        recent.append({
            "label": round_data["label"],
            "values": [{"name": r["player"], "pts": scores.get(r["player"])}
                       for r in standings[:5]]
        })
    return {"cards": cards, "comments": comments, "bars": bars,
            "recent": recent, "top_players": [r["player"] for r in standings[:5]]}
