"""Same scoreboard for the home indicators and the general standings page.

The point totals are delegated to the application's current scoring routine.
No writes and no change to the established PronoGP rules.
"""


def build_season_standings(
    weekends, load_results, load_pronos, load_reviews, compute_points,
):
    totals = {}
    gp_scored = []
    rounds = []
    for w in weekends:
        if w.get("cancelled"):
            continue
        wid = (w.get("id") or "").strip()
        if not wid:
            continue
        results = load_results(wid)
        if not results:
            continue
        gp_scored.append({"id": wid, "label": w.get("label", wid)})
        pronos_by_player = load_pronos(wid)
        reviews = load_reviews(wid)
        round_scores = {}
        for player_name, prediction in pronos_by_player.items():
            pts = compute_points(
                prediction, results, w, player_name=player_name, reviews=reviews,
            )
            detail = {
                "q": pts["qualif"]["total"],
                "s": pts["sprint"]["total"],
                "gp": pts["gp"]["total"],
                "bonus": pts["bonus"]["total"],
                "total": pts["total"],
            }
            round_scores[player_name] = detail["total"]
            if player_name not in totals:
                totals[player_name] = {
                    "player": player_name, "q": 0.0, "s": 0.0,
                    "gp": 0.0, "bonus": 0.0, "total": 0.0, "gps": 0,
                }
            score = totals[player_name]
            for key, value in detail.items():
                score[key] = round(score[key] + value, 2)
            score["gps"] += 1
        rounds.append({
            "id": wid, "label": w.get("label", wid), "scores": round_scores,
        })
    rows = list(totals.values())
    rows.sort(key=lambda r: (r["total"], r["gps"]), reverse=True)
    return rows, gp_scored, rounds


def individual_season_summary(rows, name, normalize_pseudo):
    """Choose the exact same position and total presented in the standings."""
    normalized = normalize_pseudo(name) if name else ""
    for position, row in enumerate(rows, start=1):
        if normalize_pseudo(row["player"]) == normalized:
            return {"points": row["total"], "rank": position}
    return {"points": 0.0, "rank": None}
