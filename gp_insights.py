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
        filled_count = sum(counts.values())
        # Same choice => same color *within the row*. Different answers receive
        # different colors; missing answers remain neutral. This is NOT a
        # prediction of who will score points.
        ordered_keys = sorted(counts, key=lambda key: (-counts[key], key))
        palette = ("cyan", "coral", "violet", "lime", "gold", "rose")
        choice_colors = {key: palette[i % len(palette)]
                         for i, key in enumerate(ordered_keys)}
        unanimous = filled_count >= 2 and len(counts) == 1
        cells = []
        for value in choices:
            key = _key(value)
            count = counts.get(key, 0) if value != "—" else 0
            if not count:
                status = "missing"
                signal = "Non renseigné"
            elif unanimous:
                status = "unanimous"
                signal = "Unanimité"
            elif count == 1 and filled_count >= 2:
                status = "solo"
                signal = "Choix isolé"
            elif count >= 2 and count == top:
                status = "popular"
                signal = "Choix fréquent"
            else:
                status = "alternative"
                signal = "Autre tendance"
            cells.append({
                "value": value,
                "popular": count >= 2 and count == top,
                "tone": choice_colors.get(key, "neutral"),
                "status": status,
                "signal": signal,
                "votes": count,
            })
        if unanimous:
            verdict = "Unanimité"
        elif filled_count < 2:
            verdict = "À compléter"
        elif len(counts) == filled_count:
            verdict = "Tous différents"
        else:
            verdict = f"{len(counts)} choix différents"
        matrix.append({"group": group, "label": label, "cells": cells,
                       "unique": len(counts), "verdict": verdict,
                       "unanimous": unanimous,
                       "group_key": {"Qualifications": "qualif", "Sprint": "sprint",
                                     "Grand Prix": "gp", "Questions bonus": "bonus"}[group]})

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
    comparison = {
        "unanimous": sum(line["unanimous"] for line in matrix),
        "contested": sum(line["unique"] > 1 for line in matrix),
        "isolated": sum(cell["status"] == "solo" for line in matrix
                        for cell in line["cells"]),
    }
    return {"players": players, "matrix": matrix, "distributions": focus,
            "comments": comments, "count": n, "comparison": comparison}


def build_championship_insights(standings, rounds):
    """Débrief narratif, sans tableaux ni graphiques.

    Toutes les observations proviennent du classement officiel et de l'ordre
    des manches scorées. Les absences ne sont pas comptées comme des échecs.
    """
    scored_rounds = [r for r in rounds if (r.get("scores") or {})]
    if not standings or not scored_rounds:
        return {"comments": []}

    comments = []

    def note(icon, title, message):
        comments.append({"icon": icon, "title": title, "text": message})

    lead = standings[0]
    leader = lead["player"]
    last = scored_rounds[-1]
    last_scores = last["scores"]

    # Le championnat : parler de la rivalité, plutôt que répéter des chiffres.
    if len(standings) == 1:
        note("🏆", "La course au titre",
             f"{leader} ouvre la marche pour le moment. La lutte pour le titre "
             "prendra une autre dimension lorsque d'autres joueurs seront classés.")
    else:
        second = standings[1]
        gap = round(float(lead["total"]) - float(second["total"]), 6)
        recent_best = max(
            (float(score) for race in scored_rounds[-3:]
             for score in race["scores"].values()), default=0.0,
        )
        if gap == 0:
            note("🔥", "Rien n'est joué",
                 f"{leader} et {second['player']} sont à égalité en tête. "
                 "La moindre manche réussie peut départager les prétendants.")
        elif recent_best and gap <= recent_best:
            note("🏆", "La bataille pour la tête",
                 f"{leader} mène le championnat, mais {second['player']} reste à portée. "
                 "Au vu des écarts observés sur les dernières courses, la bataille "
                 "pour la première place reste ouverte.")
        elif recent_best and gap > 2 * recent_best:
            note("🏆", "Un leader qui prend le large",
                 f"{leader} a creusé une avance solide sur {second['player']}. "
                 "Ses poursuivants vont devoir enchaîner les bonnes manches "
                 "pour revenir dans la lutte.")
        else:
            note("🏆", "La chasse au leader",
                 f"{leader} tient les commandes devant {second['player']}. "
                 "Le classement reste animé, mais le leader conserve l'avantage.")

    # La dernière manche : signaler une performance réelle, pas un prix fictif.
    top_score = max(float(value) for value in last_scores.values())
    best_players = [
        name for name, value in last_scores.items()
        if abs(float(value) - top_score) < 0.000001
    ]
    if top_score > 0:
        if len(best_players) == 1:
            winner = best_players[0]
            if winner == leader:
                detail = (f"{winner} a signé le meilleur score lors de {last['label']} "
                          "et confirme sa bonne position au championnat.")
            else:
                detail = (f"{winner} a signé le meilleur score lors de {last['label']}. "
                          "Une belle performance qui pourrait compter dans la "
                          "course aux places d'honneur.")
            note("⚡", "Le fait marquant du dernier GP", detail)
        elif len(best_players) <= 3:
            names = ", ".join(best_players)
            note("⚡", "Une manche disputée",
                 f"{names} ont partagé la meilleure performance lors de {last['label']}. "
                 "Personne n'a réussi à se détacher sur cette épreuve.")
        else:
            note("⚡", "Une manche très disputée",
                 f"Plusieurs joueurs partagent le meilleur score de {last['label']}. "
                 "La course n'a pas permis de les départager.")
    else:
        note("🏁", "Une manche sans échappée",
             f"Au {last['label']}, aucun participant n'a marqué de points. "
             "Ce résultat ne change pas les écarts du championnat.")

    # Les évolutions de place sont comparées à un vrai classement AVANT le dernier GP.
    if len(scored_rounds) >= 2:
        previous = []
        for row in standings:
            name = row["player"]
            played_last = name in last_scores
            earlier_gp = int(row["gps"]) - int(played_last)
            if earlier_gp > 0:
                previous.append((
                    name,
                    round(float(row["total"]) - float(last_scores.get(name, 0)), 6),
                    earlier_gp,
                ))
        previous.sort(key=lambda item: (-item[1], -item[2], item[0].casefold()))
        prior_ranks = {name: idx for idx, (name, _, _) in enumerate(previous, 1)}
        current_ranks = {row["player"]: idx for idx, row in enumerate(standings, 1)}
        old_leader = previous[0][0] if previous else None
        if (old_leader and old_leader != leader and
                len(standings) > 1 and
                float(lead["total"]) > float(standings[1]["total"])):
            note("🔄", "Changement de leader",
                 f"{leader} s'empare de la tête après {last['label']} et "
                 f"détrône {old_leader}. Le rapport de force vient de changer.")
        else:
            risers = [
                (prior_ranks[name] - current_ranks[name], name)
                for name in prior_ranks if name in current_ranks
                and current_ranks[name] < prior_ranks[name]
            ]
            if risers:
                _, climber = max(risers, key=lambda item: (item[0], item[1].casefold()))
                note("🚀", "Ça remonte au classement",
                     f"{climber} gagne des places au classement général après "
                     f"{last['label']}. Une progression à suivre lors du prochain GP.")

    # Une dynamique est décrite uniquement si deux périodes comparables existent.
    # Un joueur absent d'une manche est exclu de cette comparaison.
    if len(scored_rounds) >= 4:
        improving = []
        for player in (row["player"] for row in standings):
            recent = [race["scores"].get(player) for race in scored_rounds[-4:]]
            if any(value is None for value in recent):
                continue
            earlier = (float(recent[0]) + float(recent[1])) / 2
            later = (float(recent[2]) + float(recent[3])) / 2
            if later > earlier * 1.2 and later - earlier >= 1:
                improving.append((later - earlier, later, player))
        if improving:
            _, _, player = max(improving)
            note("📈", "La dynamique du moment",
                 f"{player} monte en puissance : ses deux dernières manches "
                 "sont nettement meilleures que les deux précédentes. "
                 "Une tendance à confirmer sur la suite de la saison.")

    # La régularité ne se déduit que d'au moins trois manches réellement jouées.
    if len(scored_rounds) >= 3:
        eligible = []
        for row in standings[:6]:
            player = row["player"]
            scores = [float(race["scores"][player]) for race in scored_rounds
                      if player in race["scores"]]
            if len(scores) < 3 or len(scores) < len(scored_rounds) * .6:
                continue
            mean = sum(scores) / len(scores)
            if mean <= 0:
                continue
            variation = (sum((pts - mean) ** 2 for pts in scores)
                         / len(scores)) ** .5 / mean
            eligible.append((variation, mean, player))
        if eligible:
            eligible.sort(key=lambda row: (row[0], -row[1]))
            variation, _, steady = eligible[0]
            if variation <= .45:
                note("🎯", "La régularité fait la différence",
                     f"{steady} affiche des performances particulièrement "
                     "constantes sur les manches disputées. Une solidité "
                     "précieuse pour durer au classement.")

    return {"comments": comments}
