import httpx
import uuid
import random
from datetime import datetime, timedelta
import time

N = 10000
events_url = "http://127.0.0.1:8000/events/"
analytics_url = "http://127.0.0.1:8000/stats"

events = []
for i in range(N):
    events.append({
        "event_id": str(uuid.uuid4()),
        "occurred_at": (datetime.utcnow() - timedelta(days=random.randint(0, 10))).isoformat(),
        "user_id": f"user_{random.randint(1, 1000)}",
        "event_type": f"type_{random.randint(1, 10)}",
        "properties": {"key": "value"}
    })


with httpx.Client() as client:
    # post
    start_time = time.time()
    for i in range(0, N, 1000):
        batch = events[i:i+1000]
        r = client.post(events_url, params={"dry_run": "true"}, json=batch)
        r.raise_for_status()
    end_time = time.time()
    post_time = end_time - start_time
    print(f"Sent {N} events in {post_time:.2f}s, speed={N/post_time:.0f} events/sec")

    # get
    from_date = (datetime.utcnow() - timedelta(days=7)).date().isoformat()
    to_date = datetime.utcnow().date().isoformat()

    r = client.get(f"{analytics_url}/dau", params={"from_date": from_date, "to_date": to_date})
    r.raise_for_status()
    dau_result = r.json()
    print("DAU:", dau_result)

    r = client.get(f"{analytics_url}/top-events", params={"from_date": from_date, "to_date": to_date, "limit": 5})
    r.raise_for_status()
    top_events_result = r.json()
    print("Top events:", top_events_result)

    start_date = (datetime.utcnow() - timedelta(days=7)).date().isoformat()
    r = client.get(f"{analytics_url}/retention", params={"start_date": start_date, "windows": 7})
    r.raise_for_status()
    retention_result = r.json()
    print("Retention:", retention_result)