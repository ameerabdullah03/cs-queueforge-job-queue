"""QueueForge: a persistent local queue with leasing and retry backoff."""
import argparse
import json
import sqlite3
import time
from pathlib import Path

DB = Path(__file__).with_name("queueforge.db")
MAX_ATTEMPTS = 4

def db_connect():
    c = sqlite3.connect(DB, timeout=10, isolation_level=None)
    c.row_factory = sqlite3.Row
    return c

def initialize():
    with db_connect() as c:
        c.execute("""CREATE TABLE IF NOT EXISTS jobs(
          id INTEGER PRIMARY KEY, kind TEXT NOT NULL, payload TEXT NOT NULL,
          state TEXT NOT NULL CHECK(state IN ('queued','running','done','dead')),
          attempts INTEGER NOT NULL DEFAULT 0, available_at REAL NOT NULL,
          lease_until REAL, result TEXT, error TEXT,
          created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)""")
        c.execute("CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(state, available_at, id)")

def enqueue(kind, payload):
    try:
        encoded = json.dumps(json.loads(payload), separators=(",", ":"))
    except json.JSONDecodeError as e:
        raise SystemExit(f"Payload must be valid JSON: {e}")
    with db_connect() as c:
        cur = c.execute("INSERT INTO jobs(kind,payload,state,available_at) VALUES(?,?,'queued',?)",
                        (kind, encoded, time.time()))
        return cur.lastrowid

def claim():
    now = time.time()
    with db_connect() as c:
        c.execute("BEGIN IMMEDIATE")
        c.execute("UPDATE jobs SET state='queued',available_at=?,lease_until=NULL,error='worker lease expired'
                   WHERE state='running' AND lease_until < ?", (now, now))
        job = c.execute("SELECT * FROM jobs WHERE state='queued' AND available_at<=? ORDER BY id LIMIT 1",
                        (now,)).fetchone()
        if job is None:
            c.commit()
            return None
        c.execute("UPDATE jobs SET state='running',attempts=attempts+1,lease_until=? WHERE id=?",
                  (now + 30, job["id"]))
        c.commit()
        return {**dict(job), "attempts": job["attempts"] + 1}

def handle(kind, payload):
    if kind == "greet":
        name = payload.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ValueError("greet requires a non-empty name")
        return {"message": f"Hello, {name.strip()}!"}
    if kind == "sum":
        values = payload.get("values")
        if not isinstance(values, list) or not all(isinstance(v, (int, float)) for v in values):
            raise ValueError("sum requires a numeric values array")
        return {"sum": sum(values)}
    raise ValueError(f"Unknown job kind: {kind}")

def process_one():
    job = claim()
    if not job:
        return False
    try:
        result = json.dumps(handle(job["kind"], json.loads(job["payload"])))
        with db_connect() as c:
            c.execute("UPDATE jobs SET state='done',result=?,lease_until=NULL WHERE id=? AND state='running'",
                      (result, job["id"]))
        print(f"done #{job['id']}: {result}")
    except Exception as e:
        attempts = job["attempts"]
        dead = attempts >= MAX_ATTEMPTS
        delay = min(2 ** attempts, 60)
        with db_connect() as c:
            c.execute("UPDATE jobs SET state=?,error=?,available_at=?,lease_until=NULL WHERE id=? AND state='running'",
                      ("dead" if dead else "queued", str(e)[:500], time.time() + delay, job["id"]))
        print(f"{'dead' if dead else 'retry'} #{job['id']} in {delay}s: {e}")
    return True

def main():
    initialize()
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("enqueue"); p.add_argument("kind"); p.add_argument("payload")
    p = sub.add_parser("work"); p.add_argument("--once", action="store_true")
    sub.add_parser("list")
    args = parser.parse_args()
    if args.command == "enqueue":
        print(f"enqueued job #{enqueue(args.kind, args.payload)}")
    elif args.command == "work":
        while True:
            worked = process_one()
            if args.once: break
            if not worked: time.sleep(.5)
    else:
        with db_connect() as c:
            for row in c.execute("SELECT id,kind,state,attempts,result,error FROM jobs ORDER BY id DESC"):
                print(json.dumps(dict(row), ensure_ascii=False))

if __name__ == "__main__":
    main()
