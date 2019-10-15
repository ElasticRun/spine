import json, datetime

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.cimpl import KafkaError, KafkaException

import frappe
from frappe.utils.logger import get_logger
from spine.spine_adapter.scheduler.message_processor import process_message, skip_message

logger = None


class KafkaAsyncSingletonConsumer():
    __instance = None

    def __init__(self, log=None):
        global logger
        logger = log
        if KafkaAsyncSingletonConsumer.__instance is None:
            conf = self.get_kafka_conf()
            logger.debug("Kafka configuration - {}".format(frappe.as_json(conf)))
            KafkaAsyncSingletonConsumer.__instance = KafkaAsyncSingletonConsumer.__KafkaAsyncSingletonConsumer(conf)
            logger.debug("Instance initialized - {}".format(KafkaAsyncSingletonConsumer.__instance))

    def start_dispatcher(self, queue, type, site, quiet=False):
        self.__instance.poll_for_messages(queue, type, site, quiet)

    class __KafkaAsyncSingletonConsumer(Consumer):
        loop = None
        config = None
        consumer_thread = None
        task = None

        def __init__(self, conf):
            self.config = conf.copy()
            conf.pop("consumer.min.commit.count")
            super(KafkaAsyncSingletonConsumer.__KafkaAsyncSingletonConsumer, self).__init__(conf)

        def poll_for_messages(self, queue, format_type, site, quiet=False):
            frappe.connect(site)
            global logger
            if self.config.get("consumer.min.commit.count"):
                commit_count = self.config.get("consumer.min.commit.count")

            logger.debug("Starting poll loop with commit_count - {}".format(commit_count))
            msg_count = 0
            # topics, handlers = get_enabled_topics_handlers()
            #topics = ["events.topic"]
            topics = set()
            config = frappe.get_cached_doc("Spine Consumer Config", "Spine Consumer Config").as_dict()
            # debug_flag = config.get("message_debug")
            configs = config.get("configs")
            for spine_config in configs:
                if spine_config.topic:
                    topics.add(spine_config.topic)
            topics = list(topics)
            if len(topics) == 0:
                topics = ['events.topic']
            partitions_to_assign = {}
            partitions = []
            logger.debug("Getting cluster metadata for assignment.")
            # Getting cluster metadata and partitions data for assignment
            for topic in topics:
                topic_partitions = []
                metadata = self.list_topics(topic=topic, timeout=1)
                if metadata and metadata.topics:
                    topic_metadata = metadata.topics.get(topic)
                    if topic_metadata and topic_metadata.partitions:
                        for partition in topic_metadata.partitions:
                            topic_partitions.append(partition)
                        logger.debug("Found {} partitions for topic {}".format(len(topic_partitions), topic))
                        partitions_to_assign.update({topic: topic_partitions})

            if partitions_to_assign and len(partitions_to_assign):
                for topic, partitions_for_topic in partitions_to_assign.items():
                    for partition in partitions_for_topic:
                        partitions.append(TopicPartition(topic, partition))

            self.subscribe(topics)
            logger.debug("Subscribed to topics {}".format(topics))
            self.assign(partitions)
            logger.debug("Assigned to partitions {}".format(partitions))
            positions = self.position(self.assignment())
            logger.debug("Current positions - {}".format(positions))
            logger.debug("Subscribed to topics - {}".format(topics))
            while True:
                msg = self.poll(timeout=0.5)
                if msg is None: continue
                logger.debug("Message received - {}".format(msg))
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(
                            "Error Message: topic - {}, partition - {} at offset {} with error {}".format(msg.topic(),
                                                                                                          msg.partition(),
                                                                                                          msg.offset(),
                                                                                                          msg.error()))
                        continue
                    else:
                        logger.error("Ignoring Message with error received - {}".format(msg.error()))
                        continue

                # Only process messages without any errors.
                logger.debug(
                    "Message Received - topic - {}, partition - {} at offset {} with key {}".format(msg.topic(),
                                                                                                    msg.partition(),
                                                                                                    msg.offset(),
                                                                                                    msg.key()))
                logger.debug("Message value - {}. Type - {}".format(msg.value(), type(msg.value())))
                # msg_value = msg.value()[0] if (type(msg.value()).__name__ == "tuple" and len(msg.value()) > 0) else msg.value()
                msg_value = msg.value()
                if msg_value and type(msg_value) is bytes:
                    msg_value = msg_value.decode("utf-8")
                    logger.debug("Message converted from bytes to string")
                filtered = skip_message(msg_value)
                if filtered:
                    # If this is a filtered message and need not be processed, skip the message and continue with
                    # further messages.
                    continue
                msg_doc = {"doctype": "Message Log", "direction": "Received", "status": "Pending",
                           "json_message": msg_value, "received_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                process_success = False
                try:
                    frappe.get_doc(msg_doc).insert()
                    process_success = True
                except:
                    frappe.log_error(title="Spine Consumer Message Log Error")
                    # Fall back to default processing instead of queue based processing.
                    try:
                        process_message(msg_value, queue)
                        process_success = True
                    except:
                        frappe.log_error(title="Spine Consume Message Processing Error")
                finally:
                    # Commit the DB updates done so far.
                    frappe.db.commit()

                if process_success:
                    msg_count += 1
                    logger.debug("Message Processing submitted. Current Message Count - {}".format(msg_count))
                    if msg_count % commit_count == 0:
                        logger.debug("Committing messages received so far. Message count - {}".format(msg_count))
                        try:
                            self.commit(asynchronous=False)
                        except KafkaError:
                            frappe.log_error(title="Kafka Commit Error")
                            logger.error("Error encountered while committing. Messages may have been lost.")
                            pass
                        except KafkaException:
                            frappe.log_error(title="Kafka Commit Exception")
                            logger.error("Error encountered while committing. Messages may have been lost.")
                            pass
                    frappe.db.commit()

    def get_kafka_conf(self):
        global logger
        logger.debug("Frappe site config - {}".format(frappe.as_json(frappe.local.conf)))
        kafka_conf = {"bootstrap.servers": "localhost:9092", "client.id": "default.consumer",
                      "default.topic.config": {"acks": "all"}, "enable.auto.commit": "false",
                      "consumer.min.commit.count": 1}
        kafka_conf.update(frappe.local.conf.get("kafka", {}))
        return kafka_conf
