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
def get_retention(start_date: str = Query(...), windows: int = Query(7)):
    if windows <= 0:
        raise HTTPException(status_code=400, detail="Windows must be greater than 0")

    session = SessionLocal()
    try:
        start_dt = datetime.fromisoformat(start_date).date()

        user_birthdays = select(
            Event.user_id,
            func.min(func.date(Event.occurred_at)).label("first_activity_date")
        ).group_by(Event.user_id).subquery()

        cohort_query = select(user_birthdays.c.user_id).where(
            user_birthdays.c.first_activity_date == start_dt
        )

        cohort_users = [row[0] for row in session.execute(cohort_query)]
        cohort_size = len(cohort_users)

        if cohort_size == 0:
            logger.info(f"/retention: No users found for cohort {start_date}")
            return {"start_date": start_date, "cohort_size": 0, "retention": []}

        retention = []
        for day in range(1, windows + 1):
            day_dt = start_dt + timedelta(days=day)

            returning_count_query = (
                select(func.count(func.distinct(Event.user_id)))
                .where(
                    Event.user_id.in_(cohort_users),
                    func.date(Event.occurred_at) == day_dt
                )
            )

            returning_count = session.execute(returning_count_query).scalar()
            retention.append({"day": day, "returning_users": returning_count})

        logger.info(f"/retention called with start_date={start_date}, windows={windows}, cohort_size={cohort_size}")
        return {"start_date": start_date, "cohort_size": cohort_size, "retention": retention}

    finally:
        session.close()

