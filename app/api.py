from fastapi import APIRouter
from app.schemas import EventSchema
from app.tasks import insert_events_task
import logging
import json
import time

events_router = APIRouter()

@events_router.post("/")
async def ingest_events(events: list[EventSchema]):
    task = insert_events_task.delay([e.dict() for e in events])
    return {"status": "queued", "task_id": task.id, "queued_events": len(events)}

RATE_LIMIT = 100
tokens = RATE_LIMIT
last_check = time.time()

def rate_limiter():
    global tokens, last_check
    now = time.time()
    tokens += (now - last_check)
    tokens = min(tokens, RATE_LIMIT)
    last_check = now
    if tokens < 1:
        return False
    tokens -= 1
    return True


logger = logging.getLogger("events_api")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter(json.dumps({
    "time": "%(asctime)s",
    "level": "%(levelname)s",
    "message": "%(message)s"
}))
handler.setFormatter(formatter)
logger.addHandler(handler)



