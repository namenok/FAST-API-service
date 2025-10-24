from celery_app import celery_app
from app.models import Event
from app.database import SessionLocal
from sqlalchemy.dialects.postgresql import insert

import logging

logger = logging.getLogger("events_worker")
logger.setLevel(logging.INFO)

@celery_app.task
def insert_events_task(events: list[dict]):
    session = SessionLocal()
    inserted_count = 0
    duplicate_count = 0

    for event in events:
        stmt = insert(Event).values(
            event_id=event["event_id"],
            occurred_at=event["occurred_at"],
            user_id=event["user_id"],
            event_type=event["event_type"],
            properties=event.get("properties", {})
        ).on_conflict_do_nothing(index_elements=["event_id"])

        result = session.execute(stmt)
        session.commit()
        if result.rowcount == 1:
            inserted_count += 1
        else:
            duplicate_count += 1

    logger.info(f"Batch processed: inserted={inserted_count}, duplicates={duplicate_count}")
    session.close()
    return {"inserted": inserted_count, "duplicates": duplicate_count}
