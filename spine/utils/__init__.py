import frappe

def get_kafka_conf():
    frappe.logger().debug("Frappe site config - {}".format(frappe.as_json(frappe.local.conf)))
    kafka_conf = {
        "bootstrap.servers": "localhost:9092",
        "client.id": "default.consumer",
        "default.topic.config": {
            "acks": "all",
        }
    }
    kafka_conf.update(frappe.local.conf.get("kafka", {}))
    return kafka_conf