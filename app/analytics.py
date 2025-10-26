from fastapi import APIRouter, Query, HTTPException
from sqlalchemy import func, select
from app.database import SessionLocal
from app.models import Event
from datetime import datetime, timedelta
import logging
import json

logger = logging.getLogger("analytics")
logger.setLevel(logging.INFO)

formatter = logging.Formatter(json.dumps({
    "time": "%(asctime)s",
    "level": "%(levelname)s",
    "endpoint": "%(message)s"
}))

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

file_handler = logging.FileHandler("logs/analytics.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

analytics_router = APIRouter(tags=["analytics"])


@analytics_router.get("/dau")
def get_dau(from_date: str = Query(...), to_date: str = Query(...), country: str = Query(None, description="Filter by country, e.g., UA")):
    if from_date > to_date:
        raise HTTPException(status_code=400, detail="'from_date' must be before 'to_date'")

    session = SessionLocal()
    try:
        query = (
            select(
                func.date_trunc('day', Event.occurred_at).label("day"),
                func.count(func.distinct(Event.user_id)).label("dau")
            )
            .where(Event.occurred_at >= from_date)
            .where(Event.occurred_at <= to_date)
        )

        if country:
            query = query.where(Event.properties["country"].astext == country)

        query = query.group_by("day").order_by("day")

        results = session.execute(query).all()
        logger.info(f'/dau called with from={from_date}, to={to_date}, results={len(results)}')
        return [{"day": r.day.strftime("%Y-%m-%d"), "dau": r.dau} for r in results]
    finally:
        session.close()


@analytics_router.get("/top-events")
def get_top_events(
    from_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    to_date: str = Query(..., description="End date in YYYY-MM-DD format"),
    limit: int = Query(10, description="Maximum number of events to return")
):
    if limit <= 0:
        raise HTTPException(status_code=400, detail="Limit must be greater than 0")

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
        logger.info(f'/top-events called with from={from_date}, to={to_date}, limit={limit}, results={len(results)}')
        return [{"event_type": r.event_type, "count": r.cnt} for r in results]
    finally:
        session.close()


@analytics_router.get("/retention")
def get_retention(start_date: str = Query(..., description="Cohort start date in YYYY-MM-DD format"), windows: int = Query(7, description="Number of days to track retention")):
    if windows <= 0:
        raise HTTPException(status_code=400, detail="Windows must be greater than 0")

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
        logger.info(
            f'/retention called with start_date={start_date}, windows={windows}, cohort_size={len(cohort_users)}')
        return {"start_date": start_date, "retention": retention}
    finally:
        session.close()
