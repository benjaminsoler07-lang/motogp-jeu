"""Load all GP standings inputs with a bounded number of DB round trips.

No persistent cache: admin-published results and edited predictions are reflected
on the next HTTP request. Scoring still uses the existing shared routine.
"""
from sqlalchemy import text

from season_standings import build_season_standings


def build_season_standings_batched(
    weekends, engine, ensure_db_bootstrap, compute_points,
    load_results, load_pronos, load_reviews,
):
    """Read results, latest predictions and bonus reviews in three queries.

    Fallback (without PostgreSQL) deliberately uses the existing loaders.
    """
    if not engine:
        return build_season_standings(
            weekends, load_results, load_pronos, load_reviews, compute_points,
        )

    ids = list(dict.fromkeys(
        str(w.get("id") or "").strip()
        for w in weekends
        if w and not w.get("cancelled") and str(w.get("id") or "").strip()
    ))
    if not ids:
        return [], [], []

    ensure_db_bootstrap()
    with engine.begin() as conn:
        result_rows = conn.execute(text("""
            SELECT weekend_id, payload_json
            FROM results WHERE weekend_id = ANY(:ids)
        """), {"ids": ids}).fetchall()

        result_payloads = {
            wid: dict(payload or {}) for wid, payload in result_rows if payload
        }
        scored_ids = [wid for wid in ids if result_payloads.get(wid)]
        if not scored_ids:
            return [], [], []

        # Preserve the exact DISTINCT ON semantics used by the GP detail page.
        prono_rows = conn.execute(text("""
            SELECT DISTINCT ON (
                weekend_id, COALESCE(player_norm, LOWER(BTRIM(player_name)))
            )
                weekend_id, player_name, payload_json
            FROM pronos
            WHERE weekend_id = ANY(:ids)
            ORDER BY weekend_id,
                COALESCE(player_norm, LOWER(BTRIM(player_name))),
                updated_at DESC
        """), {"ids": scored_ids}).fetchall()

        review_rows = conn.execute(text("""
            SELECT weekend_id, player_norm, question_id, decision,
                   prediction_snapshot, official_snapshot, reviewed_by,
                   reviewed_at
            FROM bonus_reviews
            WHERE weekend_id = ANY(:ids)
        """), {"ids": scored_ids}).mappings().all()

    results_by_gp = {}
    # Match load_results: keep only bonus answers for configured questions.
    weekend_by_id = {str(w.get("id") or "").strip(): w for w in weekends}
    for wid in scored_ids:
        payload = result_payloads[wid]
        payload.setdefault("bonus", {})
        questions = weekend_by_id.get(wid, {}).get("bonus_questions") or []
        allowed_ids = {q["id"] for q in questions}
        payload["bonus"] = {
            key: value for key, value in (payload.get("bonus") or {}).items()
            if key in allowed_ids
        }
        results_by_gp[wid] = payload

    pronos_by_gp = {}
    for wid, player_name, payload in prono_rows:
        pronos_by_gp.setdefault(wid, {})[player_name or "??"] = dict(payload or {})

    reviews_by_gp = {}
    for row in review_rows:
        reviews_by_gp.setdefault(row["weekend_id"], {})[
            (row["player_norm"], row["question_id"])
        ] = dict(row)

    return build_season_standings(
        weekends,
        lambda wid: results_by_gp.get(wid),
        lambda wid: pronos_by_gp.get(wid, {}),
        lambda wid: reviews_by_gp.get(wid, {}),
        compute_points,
    )
