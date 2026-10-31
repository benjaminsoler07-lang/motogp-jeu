"""Tableau de comparaison des pronostics publiés, calculé à la demande.

Aucune donnée de pronostic n'est exposée avant sa publication officielle.
Les résultats proviennent des dernières saisies par joueur en base PostgreSQL.
"""
from collections import Counter
from itertools import combinations

from flask import abort, render_template, request


BASE_FIELDS = (
    ("pole", "Pole position", "pole"),
    ("q1", "Qualifiés Q1", "q1"),
    ("sprint_p1", "Sprint · P1", "sprint"),
    ("sprint_p2", "Sprint · P2", "sprint"),
    ("sprint_p3", "Sprint · P3", "sprint"),
    ("gp_p1", "Grand Prix · P1", "gp"),
    ("gp_p2", "Grand Prix · P2", "gp"),
    ("gp_p3", "Grand Prix · P3", "gp"),
)


def _answer(prono, field):
    """Normalisation visuelle ; pour la Q1, l'ordre des deux noms ne compte pas."""
    if field == "q1":
        values = [
            str(prono.get(key) or "").strip()
            for key in ("q1_1", "q1_2")
        ]
        values = sorted((v for v in values if v), key=str.casefold)
        return " + ".join(values)
    if field.startswith("bonus:"):
        answers = prono.get("bonus") or {}
        if not isinstance(answers, dict):
            return ""
        return str(answers.get(field[6:]) or "").strip()
    return str(prono.get(field) or "").strip()


def build_comparison(pronos, bonus_questions):
    """Transforme les pronos existants en données lisibles pour la page statistiques."""
    players = sorted(pronos, key=str.casefold)
    fields = list(BASE_FIELDS)
    for q in bonus_questions or []:
        if q.get("id") and q.get("label"):
            fields.append(("bonus:" + str(q["id"]), str(q["label"]), "bonus"))

    matrix = []
    consensus_count = 0
    difference_count = 0
    outliers = []
    distributions = []

    for key, label, category in fields:
        answers = {player: _answer(pronos[player] or {}, key) for player in players}
        # Insensibilité à la casse pour la comparaison, affichage original conservé.
        normalized = {player: " ".join(value.casefold().split()) for player, value in answers.items()}
        counts = Counter(value for value in normalized.values() if value)
        num_answers = sum(counts.values())
        majority = max(counts.values(), default=0)
        unanimous = bool(len(players) > 1 and num_answers == len(players)
                         and len(counts) == 1)
        if unanimous:
            consensus_count += 1
        elif len(counts) > 1:
            difference_count += 1

        cells = []
        for player in players:
            value = answers[player]
            norm = normalized[player]
            freq = counts.get(norm, 0)
            if not value:
                state = "empty"
            elif unanimous:
                state = "consensus"
            elif freq == 1 and len(counts) > 1:
                state = "outlier"
                if majority > 1:
                    outliers.append({"player": player, "field": label, "answer": value})
            elif freq == majority and majority > 1:
                state = "majority"
            else:
                state = "normal"
            cells.append({"player": player, "value": value or "—", "state": state})

        options = []
        # Distribuer à partir des réponses réelles, sans comptabiliser les absences.
        for norm, n in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
            display = next(answers[player] for player in players if normalized[player] == norm)
            options.append({
                "label": display,
                "count": n,
                "percent": round(n * 100 / num_answers) if num_answers else 0,
            })

        matrix.append({
            "field": key, "label": label, "category": category,
            "cells": cells, "unanimous": unanimous, "answered": num_answers,
        })
        if key in ("pole", "sprint_p1", "gp_p1"):
            distributions.append({"label": label, "options": options})

    similarities = []
    for left, right in combinations(players, 2):
        shared = 0
        matching = 0
        for key, _label, _category in fields:
            first = " ".join(_answer(pronos[left] or {}, key).casefold().split())
            second = " ".join(_answer(pronos[right] or {}, key).casefold().split())
            if first and second:
                shared += 1
                matching += (first == second)
        if shared:
            similarities.append({
                "left": left, "right": right, "match": matching,
                "shared": shared, "percent": round(100 * matching / shared),
            })
    similarities.sort(key=lambda row: (-row["percent"], -row["shared"], row["left"], row["right"]))

    return {
        "players": players,
        "matrix": matrix,
        "consensus_count": consensus_count,
        "difference_count": difference_count,
        "total_fields": len(fields),
        "outliers": outliers[:6],
        "distributions": distributions,
        "similarities": similarities[:8],
    }


def register_statistics(app, services):
    get_sorted_weekends = services["get_sorted_weekends"]
    weekend_season = services["weekend_season"]
    selected_season_year = services["selected_season_year"]
    get_weekend = services["get_weekend"]
    is_pronos_public = services["is_pronos_public"]
    get_latest_pronos = services["get_latest_pronos_by_player_for_weekend"]
    current_player = services["current_player"]

    @app.route("/statistiques", endpoint="statistics_page")
    def statistics_page():
        name, _ = current_player(request)
        season = selected_season_year()
        all_weekends = get_sorted_weekends()
        seasons = sorted({weekend_season(w) for w in all_weekends}, reverse=True)
        weekends = [
            w for w in all_weekends
            if weekend_season(w) == season and not w.get("cancelled")
        ]
        weekends.reverse()  # Plus récent d'abord.

        gp_id = (request.args.get("gp") or "").strip()
        if gp_id:
            if not any(w.get("id") == gp_id for w in weekends):
                abort(404)
            selected = get_weekend(gp_id)
        else:
            selected = None
            # Préférer le dernier GP dont les pronos sont officiellement visibles.
            for w in weekends:
                if is_pronos_public(w["id"]):
                    selected = get_weekend(w["id"])
                    break
            if selected is None and weekends:
                selected = get_weekend(weekends[0]["id"])

        published = bool(selected and is_pronos_public(selected["id"]))
        comparison = None
        if published:
            pronos = get_latest_pronos(selected["id"])
            if pronos:
                comparison = build_comparison(pronos, selected.get("bonus_questions", []))

        return render_template(
            "statistics.html",
            name=name,
            weekends=weekends,
            seasons=seasons,
            season_year=season,
            selected=selected,
            published=published,
            comparison=comparison,
        )
