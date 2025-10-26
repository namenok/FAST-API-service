import json

from celery_app import celery_app
from app.models import Event
from app.database import SessionLocal
from sqlalchemy.dialects.postgresql import insert

import logging

logger = logging.getLogger("events_worker")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter(json.dumps({
    "time": "%(asctime)s",
    "level": "%(levelname)s",
    "message": "%(message)s"
}))
handler.setFormatter(formatter)
logger.addHandler(handler)

@celery_app.task
def insert_events_task(events: list[dict]):
    session = SessionLocal()
    inserted_count = 0
    duplicate_count = 0
    BATCH_SIZE = 200

    for i in range(0, len(events), BATCH_SIZE):
        batch = events[i:i + BATCH_SIZE]

        stmt = insert(Event).values(batch).on_conflict_do_nothing(index_elements=["event_id"])
        result = session.execute(stmt)
        session.commit()

        inserted_count += result.rowcount
        duplicate_count += len(batch) - result.rowcount

        logger.info(f"Worker processed batch: inserted={result.rowcount}, duplicates={len(batch)-result.rowcount}")

    logger.info(f"Total processed: inserted={inserted_count}, duplicates={duplicate_count}")
    session.close()
    return {"inserted": inserted_count, "duplicates": duplicate_count}