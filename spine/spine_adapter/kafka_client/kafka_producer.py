import datetime

from confluent_kafka import Producer

import frappe
from frappe.model.document import Document
from frappe.utils.logger import get_logger

module_name = __name__

def get_module_logger():
    return  get_logger(module_name, with_more_info=False)

def get_kafka_config():
    kafka_conf = {'bootstrap.servers': 'localhost:9092', 'client.id': 'test.producer',
                  'default.topic.config': {'acks': 'all'}}
    kafka_conf.update(frappe.local.conf.get("kafka")) if frappe.local.conf.get("kafka") else None
    return kafka_conf

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

def publish_doc_event(doc, doctype, target_topic, event, debug_flag=False, retry_count=None, retry_interval=None, args=None):
    logger = get_module_logger()

    kafka_conf = get_kafka_config()
    client_id = kafka_conf.get("client.id")

    logger.debug("Kafka configuration - {}".format(frappe.as_json(kafka_conf)))

    if kafka_conf.get("consumer.min.commit.count"):
        kafka_conf.pop("consumer.min.commit.count")

    # Ideally, this should be created as singleton
    # producer = Producer(kafka_conf)
    if isinstance(doc, Document):
        doc_to_publish = doc.as_dict()
        doc_to_publish['_doc_before_save'] = getattr(doc, '_doc_before_save', None)
    else:
        doc_to_publish = doc

    if event == 'after_rename':
        doc_to_publish['rename_meta'] = {
            'old_name': args[0],
            'new_name': args[1],
            'merge': args[2],
        }

    if retry_count and retry_interval:
        retry_payload = {"count": retry_count, "interval": retry_interval }
    else:
        retry_payload = {}

    # Create json for the event.
    event_payload = {
        "Payload": doc_to_publish,
        "Header": {
            "Origin": client_id,
            "Timestamp": datetime.datetime.now(),
            "DocType": doctype,
            "Event": event,
            "Topic": target_topic,
        },
        "Retry": retry_payload
    }
    payload = frappe.as_json(event_payload, 2)
    # Remove newlines from payload.
    # regex = r"([\\n]+)"
    # payload = re.sub(regex, "", payload, 0)
    #topic = "events.topic"
    logger.debug("Queuing message for publishing - content - {}. Length - {}".format(payload, len(payload)))
    log_document = {
        "doctype": "Message Log",
        "direction": "Sent",
        "status": "Pending",
        "json_message": str(payload),
        "event": event,
        "updated_doctype": doctype,
    }
    try:
        frappe.get_doc(log_document).insert(ignore_permissions=True)
    except:
        frappe.log_error(title="Spine Producer Message Log Error")

    # producer.produce(target_topic, payload, callback=acked)
    # producer.poll(1)
    # logger.debug("Message published to topic - {}".format(target_topic))
    # producer.flush(1)
    # logger.debug("Message polling completed")
    #
