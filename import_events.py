import csv
import json
import sys
import time
import uuid
from datetime import datetime
from sqlalchemy.dialects.postgresql import insert

from app.database import SessionLocal
from app.models import Event

BATCH_SIZE = 1000

def validate_row(row):

    try:
        event_id = str(uuid.UUID(row['event_id']))
        occurred_at = datetime.fromisoformat(row['occurred_at'])
        user_id = row['user_id']
        event_type = row['event_type']
        if not user_id or not event_type:
            raise ValueError("Empty user_id or event_type")
        properties = json.loads(row['properties_json']) if row.get('properties_json') else None
        return {
            'event_id': event_id,
            'occurred_at': occurred_at,
            'user_id': user_id,
            'event_type': event_type,
            'properties': properties
        }
    except (ValueError, json.JSONDecodeError) as e:
        raise ValueError(f"Validation error: {e}")

def import_events(csv_path, batch_key=None):
    start_time = time.time()
    print(f"Start importing events from {csv_path}")
    if batch_key:
        print(f"Batch key: {batch_key}")

    processed = 0
    inserted = 0
    skipped = 0
    batch = []

    with SessionLocal() as session, open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            processed += 1
            try:
                validated = validate_row(row)
                if batch_key:
                    validated['batch_key'] = batch_key
                batch.append(validated)
            except ValueError as e:
                print(f"Skipping row {processed}: {e}")
                skipped += 1
                continue


            if len(batch) >= BATCH_SIZE:
                batch_start = time.time()
                stmt = insert(Event).values(batch).on_conflict_do_nothing(index_elements=['event_id'])
                result = session.execute(stmt)
                session.commit()
                batch_elapsed = time.time() - batch_start

                inserted += result.rowcount
                skipped += len(batch) - result.rowcount
                print(f"Batch inserted: {result.rowcount}, skipped: {len(batch) - result.rowcount}, "
                      f"time: {batch_elapsed:.2f}s, speed: {len(batch)/batch_elapsed:.0f} rows/sec")
                batch.clear()


        if batch:
            batch_start = time.time()
            stmt = insert(Event).values(batch).on_conflict_do_nothing(index_elements=['event_id'])
            result = session.execute(stmt)
            session.commit()
            batch_elapsed = time.time() - batch_start

            inserted += result.rowcount
            skipped += len(batch) - result.rowcount
            print(f"Final batch inserted: {result.rowcount}, skipped: {len(batch) - result.rowcount}, "
                  f"time: {batch_elapsed:.2f}s, speed: {len(batch)/batch_elapsed:.0f} rows/sec")

    end_time = time.time()
    elapsed = end_time - start_time
    print(f"\nImport finished in {elapsed:.2f}s")
    print(f"Processed rows: {processed}")
    print(f"Inserted rows: {inserted}")
    print(f"Skipped rows (invalid or duplicates): {skipped}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python import_events.py <path-to-csv> [batch-key]")
        sys.exit(1)
    csv_file = sys.argv[1]
    batch_key = sys.argv[2] if len(sys.argv) > 2 else None
    import_events(csv_file, batch_key)
