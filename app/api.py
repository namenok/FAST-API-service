from fastapi import APIRouter
from app.schemas import EventSchema
from app.tasks import insert_events_task
from collections import Counter
import logging
import json
import time

events_router = APIRouter()
event_counter = Counter()

logger = logging.getLogger("events_api")
logger.setLevel(logging.INFO)

formatter = logging.Formatter(json.dumps({
    "time": "%(asctime)s",
    "level": "%(levelname)s",
    "message": "%(message)s"
}))

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

file_handler = logging.FileHandler("logs/events_api.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


@events_router.post("/")
async def ingest_events(events: list[EventSchema], dry_run: bool = False):
    if dry_run:
        return {"status": "dry_run", "queued_events": len(events)}

    event_counter["total_events_received"] += len(events)
    start_time = time.time()

    task = insert_events_task.delay([e.dict() for e in events])

    processing_time = time.time() - start_time
    logger.info(
        f"Received {len(events)} events, total={event_counter['total_events_received']}, processing_time={processing_time:.3f}s")

    return {"status": "queued", "task_id": task.id, "queued_events": len(events)}
