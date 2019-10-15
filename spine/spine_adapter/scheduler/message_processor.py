import json

import frappe
from frappe.utils.logger import get_logger
from spine.spine_adapter.redis_client.redis_client import submit_for_execution
from spine.spine_adapter.kafka_client.kafka_producer import get_kafka_config
from frappe.utils import cint
from confluent_kafka import Producer
from datetime import timedelta, datetime
import time
from spine.spine_adapter.redis_client.redis_client import submit_for_execution
from spine.spine_adapter.scheduler.error_message_processor import send_mail_for_failed_messages

module_name = __name__
logger = None

def get_module_logger():
    global logger
    if logger is not None:
        return logger
    else:
        logger = get_logger(module_name, with_more_info=False)
        return logger

def skip_message(msg_value):
    """
    Filter logic for incoming messages. Currently only checks if the messages are not produced by self.
    :param msg_value: Input message received on topic
    :return: True if message should be filtered/skipped, False otherwise.
    """
    logger = get_module_logger()
    result = False
    msg_dict = preprocess_msg(msg_value)
    client_id = msg_dict.get("Header").get("Origin")
    my_id = get_kafka_config().get("client.id")
    consumer_config = frappe.get_cached_doc("Spine Consumer Config", "Spine Consumer Config").as_dict()
    test_mode = consumer_config.get("test_mode")
    logger.debug("Filtering messages - client id - {}, my id - {}, test mode - {}".format(client_id, my_id, test_mode))
    if client_id == my_id and not test_mode:
        # Skip self-produced messages
        result = True
    logger.debug("Message Header - {}, skipped - {}".format(msg_dict.get("Header"), result))
    return result

def poll_and_process_new_messages():
    """
    Scheduled method that looks for any pending messages to be processed. Currently only 5 messages are picked up for
    processing to ensure too many messages are not picked up. Messages are sorted by time they were received at, to
    ensure processing in same order as receipt. However, this order is not guaranteed and handler implementations
    should not assume this order.
    :return: None
    """
    logger = get_module_logger()
    config = frappe.get_cached_doc("Spine Consumer Config", "Spine Consumer Config").as_dict()
    # Number of messages to pick up on one call.
    window_size = config.get("msg_window_size")
    if not window_size:
        window_size = 5
    # messages = frappe.get_all("Message Log", filters={"status": "Pending", "direction": "Received"},
    #                           fields=["*"], order_by="received_at", limit_page_length=window_size)
    messages = frappe.db.sql('''
        SELECT
            *
        from
            `tabMessage Log`
        where
            status = 'Pending'
            and direction = 'Received'
        order by
            received_at
        limit
            %(window_size)s
        for update
    ''', {
        'window_size': window_size,
    }, as_dict=True)
    if messages:
        logger.debug("Found {} unprocessed messages".format(len(messages)))
        updated_msgs = []

        for msg in messages:
            # Update status for all messages picked up for processing. This will ensure that later scheduled tasks will
            # not pick up same messages.
            updated_msgs.append(update_message_status(msg, "Processing"))
        # Commit updates
        frappe.db.commit()

        if updated_msgs and len(updated_msgs) > 0:
            for msg in updated_msgs:
                process_message_from_spine(msg)
        else:
            logger.info("SpineConsumer: No messages found for processing.")
    else:
        logger.info("SpineConsumer: No messages found for processing.")

def process_message_from_spine(msg):
    print("Name: {0}".format(msg.get("name")))
    logger = get_module_logger()
    logger.debug("Processing new message log - {} of type {}".format(msg, type(msg)))
    msg_value = msg.get("json_message")
    logger.debug("Processing new Message - {}".format(msg_value))
    status = frappe.db.sql('select status from `tabMessage Log` where name = %s for update', (msg.name,))
    status = (status and status[0][0]) or 'Not Found'
    if status != 'Processing':
        logger.debug(f'Ignoring message log {msg.get("name")} as its status is {status}')
        return
    process_success, retry, last_error = process_message(msg_value, name=msg.get('name'))
    if process_success:
        status = "Processed"
    else:
        status = "Error"
        ## The status is error and retry timeline is not returned from consumer handler
        ## Then, formulating retry timeline based on producer config
        ## The assumption is that retrying will only be used for message with direction "Received" and status with "Error"
        if not retry or len(retry) == 0:
            isRetryConfiguredInProducer = 1 if len(json.loads(msg.get("json_message")).get("Retry")) else 0
            if isRetryConfiguredInProducer:
                retryFromSource = json.loads(msg.get("json_message")).get("Retry")
                retry = timeline_for_interval(
                    msg.get("received_at"),
                    retryFromSource.get("count"),
                    retryFromSource.get("interval"),
                )
            else:
                retry = []
    update_message_status(msg, status, retry, last_error)
    # Commit DB updates.
    frappe.db.commit()

def process_message(msg_value, name=None):
    logger = get_module_logger()
    consumer_conf = frappe.get_cached_doc("Spine Consumer Config", "Spine Consumer Config")
    # Send the received message for processing on the background queue for async execution using redis
    # queue. This ensures that current event loop thread remains available for more incoming requests.
    msg_value = preprocess_msg(msg_value)
    handlers_to_enqueue = filter_handlers_for_event(msg_value, consumer_conf)

    # if not handlers_to_enqueue or len(handlers_to_enqueue) == 0:
    #     # Submit a default logging handler to ensure event is logged for analysis
    #     handlers_to_enqueue = ["spine.spine_adapter.kafka_client.kafka_async_consumer.log_doctype_event"]

    # provision for retrying.
    # To Enable, capture the retry payload sent from handler function to set status of message and value for next retry.
    retry = []
    last_error = None
    if handlers_to_enqueue and len(handlers_to_enqueue) > 0:
        # Process only if there are any handlers available. If not, do not commit.
        for handler in handlers_to_enqueue:
            logger.info("Loading {} handler".format(handler))
            func = frappe.get_attr(handler)
            try:
                func(msg_value)
                process_success = True
            except Exception as exc:
                last_error = frappe.log_error(title="Consumer Message Handler {} Error".format(handler), message={
                    'traceback': frappe.get_traceback(),
                    'message_id': name,
                })
                ## Retry array if found in the error payload then consumer would like to override the Spine retry timeline.
                retry = ((exc.args and exc.args[0]) or {}).get('error', {}).get('retry')
                frappe.db.rollback()
    else:
        # No handlers defined. Consider this as success scenario.
        process_success = True
        logger.info("No handlers defined for doctype in the message - {}".format(msg_value.get("Header")))
    return process_success, retry, last_error

def filter_handlers_for_event(msg_dict, conf):
    logger = get_module_logger()
    if msg_dict:
        logger.debug("Event payload is - {}. Type - {}".format(msg_dict, type(msg_dict)))
    header = msg_dict.get("Header", {})
    logger.debug("msg_dict type - {}, msg_dict.get-Header - {}".format(type(msg_dict), header))
    doctype = header.get("DocType")
    client_id = header.get("Origin")
    topic = header.get("Topic")
    logger.debug("""
        Header - {}. Doctype - {}. Client Id - {}
        Own Client ID from Kafka Config - {}
    """.format(header, doctype, client_id, conf.get("client.id")))
    if client_id != conf.get("client.id"):
        handlers = get_consumer_handlers(doctype, topic)
    else:
        logger.info("Ignoring self generated message as client id is same in message and local configuration.")
        handlers = []

    logger.debug("Found handlers for doctype {} = {}".format(doctype, handlers))
    return handlers

def get_consumer_handlers(doctype, topic):
    handlers = []
    frappe.connect()
    try:
        logger.debug("Retrieving configurations")
        configs = frappe.get_cached_doc("Spine Consumer Config", "Spine Consumer Config").get('configs', [])
        logger.debug("Retrieved configurations - {}".format(frappe.as_json(configs)))
        for config in configs:
            logger.debug("Comparing spine config {}:{} with doctype {}, topic {}".format(
                config.document_type,
                config.event_handler,
                doctype,
                topic,
            ))
            if config.document_type == doctype and config.event_handler and topic == config.topic:
                logger.debug("Found handlers - {}".format(config.event_handler))
                # value is expected to be comma separated list of handler functions
                handlers = [x.strip() for x in config.event_handler.split(',')]
    except:
        logger.debug("Error occurred while trying to get handlers for doctype {}.".format(doctype))
        frappe.log_error(title="Could not get handlers for doctype {}".format(doctype))
        raise
    logger.debug("Found handlers - {} for doctype - {}".format(handlers, doctype))
    return handlers

def update_message_status(msg_doc, status, retry=None, error_log=None):
    msg_doc = frappe.get_doc("Message Log", msg_doc.get("name"))
    if status == 'Error':
        if retry and len(retry) > 0:
            retrying_at = retry[0]
            retries_left = len(retry)
            msg_doc.update({
                "doctype": "Message Log",
                "status": status,
                "retrying_at": retrying_at,
                "retries_left": retries_left,
                "retrying_timeline": json.dumps(retry, default=str),
                "last_error": error_log,
            })
        else:
            status = "Failed"
            msg_doc.update({
                "doctype": "Message Log",
                "status": status,
                "last_error": error_log,
            })
            send_mail_for_failed_messages(msg_doc)
    else:
        msg_doc.update({"doctype": "Message Log", "status": status})

    return frappe.get_doc(msg_doc).save()


def timeline_for_interval(received_at, count, interval):
    count = cint(count)
    timeline = []
    while count != 0:
        if interval and interval == 'Every Hour':
            received_at = received_at + timedelta(hours=1)
        elif interval and interval == 'Every 2 Hours':
            received_at = received_at + timedelta(hours=2)
        elif interval and interval == 'Every 6 Hours':
            received_at = received_at + timedelta(hours=6)
        timeline.append(received_at)
        count = count - 1
    return timeline

def poll_and_publish_new_messages():
    """
        Method to poll for any new messages being saved to message log with direction = Sent. If any such messages are
        found, they are published onto the corresponding topic configured for that doctype.
    """
    logger = get_module_logger()
    producer = get_producer()

    # Start Add: ajitp - added to make window size cofigurable.
    config = frappe.get_cached_doc("Spine Producer Config", "Spine Producer Config").as_dict()
    # Number of messages to pick up on one call.
    window_size = config.get("msg_window_size")
    if not window_size:
        window_size = 5
    #window_size = 20
    # End add
    # messages = frappe.get_all("Message Log", filters={"status": "Pending", "direction": "Sent"},
    #                           fields=["*"], order_by="received_at", limit_page_length=window_size)
    messages = frappe.db.sql('''
        SELECT
            *
        from
            `tabMessage Log`
        where
            status = "Pending"
            and direction = "Sent"
        limit %(window_size)s
        for update
    ''', {
        'window_size': window_size,
    }, as_dict=True)

    if messages and len(messages) > 0:
        logger.debug("Found {} unprocessed messages".format(len(messages)))
        updated_msgs = []

        for msg in messages:
            # Update status for all messages picked up for processing. This will ensure that later scheduled tasks will
            # not pick up same messages.
            updated_msgs.append(update_message_status(msg, "Processing"))
        # Commit updates
        frappe.db.commit()

        for msg in updated_msgs:
            logger.debug("Processing new message log - {} of type {}".format(msg, type(msg)))
            publish_message_to_spine(msg)
        msg_count = producer.poll(timeout=0)
        producer.flush()
        # if (msg_count != window_size):
        #     frappe.log_error("All messages were not published successfully. Actual Count - {}, expected count - {}".format(msg_count, window_size))
        logger.debug("Message Count - {}".format(msg_count))
        frappe.db.commit()
    else:
        logger.info("SpineProducer: No messages found for processing.")

def get_producer():
    if hasattr(frappe.local, 'spine_producer'):
        return frappe.local.spine_producer

    kafka_conf = get_kafka_config()
    logger.debug("Kafka configuration - {}".format(frappe.as_json(kafka_conf)))

    if kafka_conf.get("consumer.min.commit.count"):
        kafka_conf.pop("consumer.min.commit.count")

    # Ideally, this should be created as singleton
    producer = frappe.local.spine_producer = Producer(kafka_conf)
    return producer

def publish_message_to_spine(msg, bulk=True):
    logger = get_module_logger()
    logger.debug("Processing new message log - {} of type {}".format(msg, type(msg)))
    status = frappe.db.sql('select status from `tabMessage Log` where name = %s for update', (msg.name,))
    status = (status and status[0][0]) or 'Not Found'
    if status != 'Processing':
        logger.debug(f'Ignoring message log {msg.get("name")} as its status is {status}')
        return
    msg_value = msg.get("json_message")
    msg_value = preprocess_msg(msg_value)
    target_topic = msg_value.get("Header").get("Topic")
    logger.debug("Processing new Message - {}".format(msg_value))
    # Publish message onto Kafka
    status = 'Processed'
    error_log = None
    try:
        producer = get_producer()
        producer.produce(target_topic, json.dumps(msg_value), callback=acked)
        if not bulk:
            logger.debug("Flushing message to spine")
            msg_count = producer.flush(timeout=20)
            if msg_count:
                raise Exception('Producer flush timed out')
        logger.debug("Message published to topic - {}".format(target_topic))
    except Exception:
        status = 'Error'
        error_log = frappe.log_error(
            frappe.get_traceback(),
            f'Error while publishing {msg.get("name")}',
        ).name
    update_message_status(msg, status, None, error_log)
    frappe.db.commit()

def preprocess_msg(msg):
    if isinstance(msg, dict):
        return msg

    if msg and type(msg).__name__ == "bytes":
        msg = msg.decode("utf-8")
        logger.debug("Message converted from bytes to string")
    try:
        msg_dict = msg
        while type(msg_dict) is str:
            msg_dict = json.loads(msg_dict)
        logger.debug("Payload converted to dict - {} with type {}".format(msg_dict, type(msg_dict)))
    except:
        msg_dict = msg
        logger.debug("Payload could not be converted to dict")
        frappe.log_error(title="Message could not be converted")
    return msg_dict

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))
