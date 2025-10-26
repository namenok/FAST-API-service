from fastapi import FastAPI, Request, HTTPException
from app.api import events_router
from app.analytics import analytics_router
import time

app = FastAPI()

RATE_LIMIT = 100
tokens = RATE_LIMIT
last_check = time.time()

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    global tokens, last_check
    now = time.time()

    tokens += (now - last_check)
    tokens = min(tokens, RATE_LIMIT)
    last_check = now

    if tokens < 1:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    tokens -= 1
    start_time = time.time()
    response = await call_next(request)
    response.headers["X-Process-Time"] = str(time.time() - start_time)
    return response

app.include_router(events_router, prefix="/events")
app.include_router(analytics_router, prefix="/stats")

@app.get("/")
def read_root():
    return {"Hello": "Welcome to the Analytics API"}