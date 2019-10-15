import json

import frappe
from frappe.utils.logger import get_logger
from datetime import timedelta, datetime
from frappe.utils import cint, cstr
from frappe import _

from spine.spine_adapter.redis_client.redis_client import submit_for_execution

module_name = __name__
logger = None

def date_diff_in_Seconds(dt2, dt1):
    timedelta = dt2 - dt1
    return timedelta.days * 24 * 3600 + timedelta.seconds

def poll_and_process_error_messages():
    """
    Scheduled method that looks for any error messages to be processed with retry timelineconfigured.The messages will be picked based on it next schedule.     
    Currently only 5 messages are picked up for processing to ensure too many messages are not picked up. Messages are sorted by time they were received at, to
    ensure processing in same order as receipt. However, this order is not guaranteed and handler implementations
    should not assume this order.
    :return: None
    """
    global logger
    get_module_logger()
    config = frappe.get_cached_doc("Spine Consumer Config", "Spine Consumer Config").as_dict()
    # Number of messages to pick up on one call.
    window_size = config.get("msg_window_size")
    if not window_size:
        window_size = 5
    messages = frappe.get_all("Message Log", filters={"status": "Error", "direction": "Received"},
                              fields=["*"], order_by="retrying_at", limit_page_length=window_size)

    if messages and len(messages) > 0:
        ## Filter all messages from the messages list based on next retry timestamp
        ## Processing messages for only whose next retry timestamp have passed
        logger.debug("Found {} error messages".format(len(messages)))
        print("pending msgs found: {0}".format(len(messages)))
        updated_msgs = []

        for msg in messages:
            print("name: {0}".format(msg.get("name")))
            print("current date: {0}".format(datetime.now()))
            print("retrying at: {0}".format(msg.get("retrying_at")))
            print("time diff: {0}".format(date_diff_in_Seconds(datetime.now(), msg.get('retrying_at'))))
            if date_diff_in_Seconds(datetime.now(), msg.get('retrying_at')) > 0:
                # messages.remove(msg)
            # else:
                # Update status for all messages picked up for processing. This will ensure that later scheduled tasks will
                # not pick up same messages.
                updated_msgs.append(update_message_status(msg, "Processing"))
            # Commit updates
            frappe.db.commit()
        for msg in updated_msgs:
            status = None
            retries_left = cint(msg.get("retries_left")) - 1
            retry = json.loads(msg.get("retrying_timeline"))
            if retries_left >= 0:
                logger.debug("Processing new message log - {} of type {}".format(msg, type(msg)))
                msg_value = msg.get("json_message")
                logger.debug("Processing new Message - {}".format(msg_value))
                # Process synchronously without submitting to redis queue. This can be changed later.
                process_success = process_message(msg_value)
                if process_success:
                    status = "Processed"
                else:
                    status = "Error"
            else:
                status = "Failed"
            update_message_status(msg, status, retry=retry, retries_left=retries_left)
            # Commit DB updates.
            frappe.db.commit()
    else:
        logger.info("SpineConsumer: No messages found for processing.")

def get_module_logger():
    global logger
    if logger is not None :
        return logger
    else:
        logger = get_logger(module_name, with_more_info=False)
        return logger


def preprocess_msg(msg):
    if msg and type(msg).__name__ == "bytes":
        msg = msg.decode("utf-8")
        logger.debug("Message converted from bytes to string")
    try:
        #msg_dict = json.loads(msg)
        msg_dict = msg
        while type(msg_dict) is str:
            msg_dict = json.loads(msg_dict)
        logger.debug("Payload converted to dict - {} with type {}".format(msg_dict, type(msg_dict)))
    except:
        msg_dict = msg
        logger.debug("Payload could not be converted to dict")
        frappe.log_error(title="Message could not be converted")
    return msg_dict


def filter_handlers_for_event(msg, conf):
    global logger
    get_module_logger()
    if msg:
        logger.debug("Event payload is - {}. Type - {}".format(msg, type(msg)))
    doctype = None
    msg_dict = msg
    
    try:
        client_id = None
        doctype = None
        logger.debug("msg_dict type - {}, msg_dict.get-Header - {}".format(type(msg_dict), msg_dict.get("Header")))
        if msg_dict.get("Header") and msg_dict.get("Header").get("DocType"):
            doctype = msg_dict.get("Header").get("DocType")
            client_id = msg_dict.get("Header").get("Origin")
            logger.debug(
                "Header - {}. Doctype - {}".format(msg_dict.get("Header"), msg_dict.get("Header").get("DocType")))
        logger.debug("DocType found from message - {}".format(doctype))
        logger.debug("Client ID found from message - {}".format(client_id))
        logger.debug("Own Client ID from Kafka Config - {}".format(conf.get("client.id")))
        if client_id != conf.get("client.id"):
            handlers = get_consumer_handlers(doctype)
        else:
            logger.info("Ignoring self generated message as client id is same in message and local configuration.")
            handlers = []

    except:
        handlers = []
        frappe.log_error(title="Spine Handler Lookup Error")
    logger.debug("Found handlers for doctype {} = {}".format(doctype, handlers))
    return handlers


def process_message(msg_value, queue = None):
    global logger
    get_module_logger()
    process_success = False
    consumer_conf = frappe.get_cached_doc("Spine Consumer Config", "Spine Consumer Config").as_dict()
    # Send the received message for processing on the background queue for async execution using redis
    # queue. This ensures that current event loop thread remains available for more incoming requests.
    msg_value = preprocess_msg(msg_value)
    handlers_to_enqueue = filter_handlers_for_event(msg_value, consumer_conf)

    # if not handlers_to_enqueue or len(handlers_to_enqueue) == 0:
    #     # Submit a default logging handler to ensure event is logged for analysis
    #     handlers_to_enqueue = ["spine.spine_adapter.kafka_client.kafka_async_consumer.log_doctype_event"]

    # provision for retrying.
    # To Enable, capture the retry payload sent from handler function to set status of message and value for next retry. 

    if handlers_to_enqueue and len(handlers_to_enqueue) > 0:
        # Process only if there are any handlers available. If not, do not commit.
        for handler in handlers_to_enqueue:
            try:
                logger.info("Loading {} handler".format(handler))
                func = frappe.get_attr(handler)
                # If queue is provided, use async execution, else execute synchronously
                if queue:
                    try:
                        submit_for_execution(queue, func, msg_value)
                        process_success = True
                    except:
                        frappe.log_error(title="Consumer Async Handler {} Submission Error".format(handler))
                else:
                    try:
                        func(msg_value)
                        process_success = True
                    except Exception as exc:
                        frappe.log_error(title="Consumer Message Handler {} Error".format(handler))
                logger.debug("Enqueued {} for processing of event.".format(handler))
            except:
                frappe.log_error(
                    title="Could not load handler {}. Ignoring.".format(handler))
    else:
        # No handlers defined. Consider this as success scenario.
        process_success = True
        logger.info("No handlers defined for doctype in the message - {}".format(msg_value.get("Header")))
    return process_success


def get_consumer_handlers(doctype):
    handlers = []
    frappe.connect()
    try:
        # config = frappe.get_single("SpineConsumerConfig")
        logger.debug("Retrieving configurations")
        config = frappe.get_cached_doc("Spine Consumer Config", "Spine Consumer Config").as_dict()
        logger.debug("Retrieved consumer configuration - {}".format(config))
        configs = config.get("configs")
        logger.debug("Retrieved configurations - {}".format(configs))
        if configs:
            logger.debug("Checking {} configurations".format(len(configs)))
            for spine_config in configs:
                logger.debug("Comparing spine config {}:{} with doctype {}".format(spine_config.document_type,
                                                                                   spine_config.event_handler, doctype))
                if spine_config.document_type == doctype and spine_config.event_handler:
                    logger.debug("Found handlers - {}".format(spine_config.event_handler))
                    # value is expected to be comma separated list of handler functions
                    handlers = [x.strip() for x in spine_config.event_handler.split(',')]
    except:
        logger.debug("Error occurred while trying to get handlers for doctype {}.".format(doctype))
        frappe.log_error(title="Could not get handlers for doctype {}".format(doctype))
        pass
    logger.debug("Found handlers - {} for doctype - {}".format(handlers, doctype))
    return handlers


def update_message_status(msg_doc, status, retry=None, retries_left=None):
    if status == 'Error':
        retry_index = len(retry) - retries_left
        if retry_index != len(retry):            
            retrying_at = datetime.strptime(retry[retry_index], '%Y-%m-%d %H:%M:%S')
            msg_doc.update({"doctype": "Message Log", "status": status, "retrying_at": retrying_at, "retries_left": retries_left})
        else:
            status = 'Failed'
            msg_doc.update({"doctype": "Message Log", "status": status, "retries_left": retries_left})
            send_mail_for_failed_messages(msg_doc)
    else:
        msg_doc.update({"doctype": "Message Log", "status": status})
    updated_msg = frappe.get_doc(msg_doc).save()
    return updated_msg

def send_mail_for_failed_messages(msg_doc):
    msg_doc = frappe.get_doc(msg_doc).as_dict()
    json_message = json.loads(msg_doc.get("json_message"))

    recipients = ["amit.anand@elastic.run"] 
    frappe.sendmail(recipients=recipients,
				subject=_("Syncing of Message failed for document {0}".format(json_message.get("Header").get("DocType"))),
				message="Please check with the support team.",
				header=['Sync Failed Notification', 'red']
			)


