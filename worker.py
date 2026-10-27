"""Order Worker: consumes order messages, processes them, then ACKs.

Run two of these at once to see competing consumers:
  python worker.py --name worker-1
  python worker.py --name worker-2

Failure demo (crash after receiving, BEFORE the ACK):
  python worker.py --name worker-1 --crash-before-ack

Idempotency:
  Processed event_ids are stored in state/processed_events.txt, a file both
  worker processes share. Before doing the business action, a worker checks
  that file; if the event_id is already there, it logs DUPLICATE, skips the
  action, and just ACKs so the message leaves the queue.
"""
import argparse
import fcntl
import json
import os
import time
from pathlib import Path

from common import QUEUE_NAME, connect, get_logger

STATE_DIR = Path(__file__).parent / "state"
PROCESSED_FILE = STATE_DIR / "processed_events.txt"   # dedup store (event_ids)
LEDGER_FILE = STATE_DIR / "inventory_ledger.txt"      # the "business action" output
LOCK_DIR = STATE_DIR / "locks"


def is_processed(event_id):
    if not PROCESSED_FILE.exists():
        return False
    return event_id in PROCESSED_FILE.read_text().split()


def mark_processed(event_id):
    with PROCESSED_FILE.open("a") as f:
        f.write(event_id + "\n")
        f.flush()
        os.fsync(f.fileno())


def do_business_action(order, worker_name):
    """Pretend to reserve inventory. Appends one line per processed order."""
    with LEDGER_FILE.open("a") as f:
        f.write(f"{order['event_id']} {order['order_id']} reserve "
                f"{order['quantity']}x{order['item_id']} by {worker_name}\n")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--name", default=f"worker-{os.getpid()}")
    p.add_argument("--work-seconds", type=float, default=1.0, help="simulated processing time")
    p.add_argument("--crash-before-ack", action="store_true",
                   help="exit abruptly on the first message, after receiving it but before ACK")
    args = p.parse_args()

    log = get_logger(args.name)
    LOCK_DIR.mkdir(parents=True, exist_ok=True)

    conn, ch = connect()
    # prefetch_count=1: the broker gives this worker only ONE unacked message at a time,
    # so work is spread across workers instead of one worker hoarding the queue.
    ch.basic_qos(prefetch_count=1)

    def on_message(channel, method, properties, body):
        order = json.loads(body)
        eid = order["event_id"]
        short = eid[:8]
        log.info("RECEIVED event_id=%s order_id=%s item_id=%s qty=%d delivery_tag=%d redelivered=%s",
                 short, order["order_id"], order["item_id"], order["quantity"],
                 method.delivery_tag, method.redelivered)

        # Per-event file lock: if two copies of the same event arrive at both workers
        # at the same moment, only one can be inside this block. The OS releases the
        # lock automatically if the process dies, so a crash never leaves it stuck.
        with open(LOCK_DIR / f"{eid}.lock", "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)

            if is_processed(eid):
                log.warning("DUPLICATE detected event_id=%s order_id=%s -> skipping business action",
                            short, order["order_id"])
                channel.basic_ack(delivery_tag=method.delivery_tag)
                log.info("ACKED duplicate event_id=%s (removed from queue, no side effects)", short)
                return

            log.info("PROCESSING order_id=%s: reserving %d x %s ...",
                     order["order_id"], order["quantity"], order["item_id"])
            time.sleep(args.work_seconds)

            if args.crash_before_ack:
                log.error("SIMULATED CRASH before ACK for event_id=%s -> exiting now, NO ACK sent", short)
                os._exit(1)   # hard exit: no ack, no clean connection close (like kill -9)

            do_business_action(order, args.name)
            mark_processed(eid)

        channel.basic_ack(delivery_tag=method.delivery_tag)   # ACK only after processing finishes
        log.info("DONE + ACKED event_id=%s order_id=%s", short, order["order_id"])

    # auto_ack=False -> messages stay "unacked" until we call basic_ack ourselves.
    ch.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message, auto_ack=False)
    log.info("Waiting for messages on queue '%s' (prefetch=1). Ctrl+C to stop.", QUEUE_NAME)
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        log.info("Stopping.")
        ch.stop_consuming()
        conn.close()


if __name__ == "__main__":
    main()
