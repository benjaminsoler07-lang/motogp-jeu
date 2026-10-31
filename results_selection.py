"""Choose the most relevant Grand Prix on the results landing page.

No live schedule lookup is required. The weekend dates and session times are
read from the existing season calendar. Explicit GP links always take priority
over this automatic suggestion.
"""
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo


PARIS = ZoneInfo("Europe/Paris")


def _gp_day(weekend):
    day = weekend.get("date_obj")
    if isinstance(day, datetime):
        return day.date()
    if isinstance(day, date):
        return day
    try:
        return date.fromisoformat(str(weekend.get("date") or ""))
    except (TypeError, ValueError):
        return None


def _first_session_day(weekend):
    days = []
    for session in weekend.get("sessions") or []:
        if not isinstance(session, dict):
            continue
        try:
            start = datetime.fromisoformat(
                str(session.get("utc") or "").replace("Z", "+00:00")
            )
        except (ValueError, TypeError):
            continue
        if start.tzinfo is not None:
            days.append(start.astimezone(PARIS).date())
    return min(days) if days else None


def default_results_weekend(weekends, *, now=None):
    """Select current race weekend, otherwise most recently completed GP.

    On Fridays/Saturdays, the Sunday GP should already be the default, even
    without official results. An explicitly configured earlier practice session
    can extend the opening of that race weekend. An upcoming race outside the
    weekend never displaces a previous completed GP.

    On seasons with no raced GP yet, the nearest upcoming event is shown.
    Cancelled and undated events are ignored.
    """
    today = (now or datetime.now(PARIS)).astimezone(PARIS).date()
    eligible = [
        (day, w)
        for w in weekends
        if not w.get("cancelled") and (day := _gp_day(w)) is not None
    ]
    if not eligible:
        return None

    active = []
    for day, weekend in eligible:
        start = day - timedelta(days=2)
        first_session = _first_session_day(weekend)
        if first_session is not None:
            start = min(start, first_session)
        if start <= today <= day:
            active.append((day, weekend))
    if active:
        return max(active, key=lambda item: item[0])[1]

    completed = [(day, w) for day, w in eligible if day <= today]
    if completed:
        return max(completed, key=lambda item: item[0])[1]

    return min(eligible, key=lambda item: item[0])[1]
