from fastapi import APIRouter, Query
from sqlalchemy import func, select


from app.database import SessionLocal
from app.models import Event
from datetime import datetime, timedelta

analytics_router = APIRouter(tags=["analytics"])


@analytics_router.get("/dau")
def get_dau(from_date: str = Query(...), to_date: str = Query(...)):
    session = SessionLocal()
    try:
        query = (
            select(
                func.date_trunc('day', Event.occurred_at).label("day"),
                func.count(func.distinct(Event.user_id)).label("dau")
            )
            .where(Event.occurred_at >= from_date)
            .where(Event.occurred_at <= to_date)
            .group_by("day")
            .order_by("day")
        )
        results = session.execute(query).all()
        return [{"day": r.day.strftime("%Y-%m-%d"), "dau": r.dau} for r in results]
    finally:
        session.close()


@analytics_router.get("/top-events")
def get_top_events(
    from_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    to_date: str = Query(..., description="End date in YYYY-MM-DD format"),
    limit: int = Query(10, description="Maximum number of events to return")
):
    session = SessionLocal()
    try:
        query = (
            select(Event.event_type, func.count().label("cnt"))
            .where(Event.occurred_at >= from_date)
            .where(Event.occurred_at <= to_date)
            .group_by(Event.event_type)
            .order_by(func.count().desc())
            .limit(limit)
        )
        results = session.execute(query).all()
        return [{"event_type": r.event_type, "count": r.cnt} for r in results]
    finally:
        session.close()


@analytics_router.get("/retention")
def get_retention(start_date: str = Query(..., description="Cohort start date in YYYY-MM-DD format"), windows: int = Query(7, description="Number of days to track retention")):
    session = SessionLocal()
    try:
        start_dt = datetime.fromisoformat(start_date)
        end_dt = start_dt + timedelta(days=windows)

        cohort_users = session.execute(
            select(Event.user_id)
            .where(func.date(Event.occurred_at) == start_dt.date())
        ).scalars().all()

        retention = []
        for day in range(1, windows + 1):
            day_dt = start_dt + timedelta(days=day)
            returning_count = session.execute(
                select(func.count(func.distinct(Event.user_id)))
                .where(Event.user_id.in_(cohort_users))
                .where(func.date(Event.occurred_at) == day_dt.date())
            ).scalar()
            retention.append({"day": day, "returning_users": returning_count})

        return {"start_date": start_date, "retention": retention}
    finally:
        session.close()
