"""Order Producer: publishes order messages to the RabbitMQ 'orders' queue.

Examples:
  python producer.py --count 6                 # 6 new orders
  python producer.py --count 1                 # 1 order (used for the crash demo)
  python producer.py --count 3 --duplicate     # 3 orders, then re-send the 1st one (same event_id)
  python producer.py --purge                   # empty the queue and exit
"""
import argparse
import json
import random
import uuid

import pika

from common import QUEUE_NAME, connect, get_logger

log = get_logger("producer")
ITEMS = ["SKU-APPLE", "SKU-BANANA", "SKU-COFFEE", "SKU-MILK", "SKU-BREAD"]


def make_order():
    return {
        "event_id": str(uuid.uuid4()),          # unique per event -> used for dedup
        "order_id": f"ORD-{random.randint(1000, 9999)}",
        "item_id": random.choice(ITEMS),
        "quantity": random.randint(1, 5),
    }


def publish(ch, order, note=""):
    ch.basic_publish(
        exchange="",                 # default exchange routes by queue name
        routing_key=QUEUE_NAME,
        body=json.dumps(order),
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,  # message survives broker restart
            content_type="application/json",
            message_id=order["event_id"],
        ),
    )
    log.info("PUBLISHED%s event_id=%s order_id=%s item_id=%s quantity=%d",
             note, order["event_id"][:8], order["order_id"], order["item_id"], order["quantity"])


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--count", type=int, default=5, help="number of new orders to publish")
    p.add_argument("--duplicate", action="store_true",
                   help="after publishing, re-send the first order with the SAME event_id")
    p.add_argument("--purge", action="store_true", help="purge the queue and exit")
    args = p.parse_args()

    conn, ch = connect()
    if args.purge:
        ch.queue_purge(QUEUE_NAME)
        log.info("Purged queue '%s'", QUEUE_NAME)
        conn.close()
        return

    sent = []
    for _ in range(args.count):
        order = make_order()
        publish(ch, order)
        sent.append(order)

    if args.duplicate and sent:
        publish(ch, sent[0], note=" (DUPLICATE re-send)")

    log.info("Done. %d message(s) sent to queue '%s'",
             len(sent) + (1 if args.duplicate and sent else 0), QUEUE_NAME)
    conn.close()


if __name__ == "__main__":
    main()
