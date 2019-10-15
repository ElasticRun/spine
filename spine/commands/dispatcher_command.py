import logging
import os
from logging.handlers import RotatingFileHandler

import click

import frappe
from spine.spine_adapter.kafka_client.kafka_async_consumer import KafkaAsyncSingletonConsumer

loggers = {}
default_log_level = logging.DEBUG
LOG_FILENAME = '../logs/event_dispatcher.log'

logger = None


def get_logger(module):
    if module in loggers:
        return loggers[module]

    formatter = logging.Formatter('[%(levelname)s] %(asctime)s | %(pathname)s:%(message)s\n')
    # handler = logging.StreamHandler()

    handler = RotatingFileHandler(
        LOG_FILENAME, maxBytes=100000, backupCount=20)
    handler.setFormatter(formatter)

    logger = logging.getLogger(module)
    logger.setLevel(frappe.log_level or default_log_level)
    logger.addHandler(handler)
    logger.propagate = False

    loggers[module] = logger

    return logger


def start_dispatchers(site, queue="kafka_events", type="json", quiet=False, **kargs):
    global logger
    logger = get_logger(__name__)
    logger.debug("Starting worker process.")
    while True:
        dispatcher(site, queue, type, quiet, logger)


def dispatcher(site, queue, type="json", quiet=False, log=None):
    frappe.init(site=site)
    frappe_logger = frappe.logger(__name__, with_more_info=False)
    frappe_logger.info(
        "Event Dispatcher starting with pid {}. Process will log to {}".format(os.getpid(), LOG_FILENAME))

    try:
        logger.debug("Starting dispatcher worker.")
        KafkaAsyncSingletonConsumer(log=log).start_dispatcher(queue, type, site, quiet)
    finally:
        frappe.destroy()
