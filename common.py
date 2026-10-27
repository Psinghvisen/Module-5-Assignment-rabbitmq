"""Shared settings for the producer and worker."""
import logging
import os

import pika

RABBIT_HOST = os.getenv("RABBIT_HOST", "localhost")
QUEUE_NAME = os.getenv("QUEUE_NAME", "orders")


def connect():
    """Open a connection + channel and make sure the queue exists.

    durable=True means the queue survives a broker restart.
    Both producer and worker declare it, so whichever starts first creates it.
    """
    conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE_NAME, durable=True)
    return conn, ch


def get_logger(name):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("pika").setLevel(logging.WARNING)
    return logging.getLogger(name)
