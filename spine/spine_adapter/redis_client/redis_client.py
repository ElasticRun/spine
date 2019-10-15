import json
import logging
import multiprocessing
import os
import pickle
import uuid
from logging.handlers import RotatingFileHandler

import redis
from rq import Connection
from rq.logutils import setup_loghandlers

import frappe

default_log_level = logging.DEBUG
LOG_FILENAME = '../logs/json_worker.log'

logger = None

redis_connection = None
queues = {}
loggers = {}


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

def get_redis_conn():
    if not hasattr(frappe.local, 'conf'):
        raise Exception('You need to call frappe.init')

    elif not frappe.local.conf.redis_queue:
        raise Exception('redis_queue missing in common_site_config.json')

    global redis_connection

    if not redis_connection:
        redis_connection = redis.from_url(frappe.local.conf.redis_queue)

    return redis_connection


def get_redis_queue(queue):
    conn = get_redis_conn()
    global queues
    if not queue in queues:
        queues.update({queue: SimpleQueue(conn, queue)})
    return queues.get(queue)


def submit_for_execution(queue, func, *args):
    get_redis_queue(queue).enqueue(func, args)


class SimpleQueue(object):
    def __init__(self, conn, name):
        self.conn = conn
        self.name = name

    def enqueue(self, func, *args):
        task = SimpleTask(func, *args)
        serialized_task = pickle.dumps(task, protocol=pickle.HIGHEST_PROTOCOL)
        self.conn.lpush(self.name, serialized_task)
        return task.id

    def dequeue_and_execute(self, logging_level, log, site):
        setup_loghandlers(logging_level)
        _, serialized_task = self.conn.brpop(self.name, timeout=0)
        if serialized_task:
            task = pickle.loads(serialized_task)
            try:
                task.process_task(log, site)
                return task
            except Exception as e:
                frappe.db.rollback()
                frappe.flags.enqueue_after_commit = []
                frappe.logger().error(str(e))
                frappe.logger().error(frappe.get_traceback())

    def get_length(self):
        return self.conn.llen(self.name)


class SimpleTask(object):
    def __init__(self, func, *args):
        self.id = str(uuid.uuid4())
        self.func = func
        self.args = args

    def process_task(self, log, site):
        # Pass only first input, ignore others as we want to pass on only the payload and nothing else.
        log.debug("Executing function {} as part of task execution".format(self.func))
        log.debug("type(self.args) - {}".format(type(self.args).__name__))
        input = self.args
        while type(input).__name__ == "tuple":
            input = input[0]
            log.debug("Input type - {}".format(type(input).__name__))

        # input = self.args[0] if (type(self.args).__name__ == "tuple") and len(self.args) > 0 else self.args
        if input and type(input).__name__ == "bytes":
            input = input.decode("utf-8")
            log.debug("Converted message to String.")
        input = json.loads(input)
        log.debug("Type of input - {}".format(type(input)))
        # if type(input).__name__ == "bytes":
        #     input = json.loads(input)
        self.func(input)
        log.debug("Completed function execution for {}".format(self.func))
        frappe.db.commit()


def worker(site, queue, type="json", quiet=False, log=None):
    global logger
    logger = log
    try:
        frappe.connect(site=site)
        frappe_logger = frappe.logger(__name__, with_more_info=False)
        frappe_logger.info("Redis Worker starting with pid {}. Process will log to {}".format(os.getpid(), LOG_FILENAME))

        # empty init is required to get redis_queue from common_site_config.json
        redis_connection = get_redis_conn()
        if os.environ.get('CI'):
            setup_loghandlers('ERROR')
        with Connection(redis_connection):
            logging_level = "INFO"
            if quiet:
                logging_level = "WARNING"
        q = get_redis_queue(queue)
        # if q.get_length() > 0:
        q.dequeue_and_execute(logging_level, log, site)
        # else:
        #    logger.debug("No tasks available in the queue")
    finally:
        frappe.destroy()


def start_workers(site, queue="kafka_events", type="json", quiet=False, **kargs):
    """
    Starts processes for workers depending on the kargs passed
    :param num_workers - integer value to indicate the number of workers to be started.
    :param queue - name of the queue to listen to for jobs/instructions
    :param kargs: key value pairs used for configuration of workers. Following keys are used
    :return: None
    """
    global logger
    logger = get_logger(__name__)
    processes = []
    logger.debug("Starting worker process.")
    while True:
        worker(site, queue, type, quiet)
