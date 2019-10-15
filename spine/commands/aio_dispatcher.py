import asyncio
import frappe
from aiokafka import AIOKafkaConsumer
from spine.utils import get_kafka_conf
from spine.utils.command_controller import start_command_handler

def start_dispatchers(site, queue):
    frappe.init(site=site)
    frappe.connect()
    fetch_meta_and_run(queue)

def fetch_meta_and_run(queue):
    loop = asyncio.get_event_loop()
    consumer_config = frappe.get_cached_doc('Spine Consumer Config', 'Spine Consumer Config')
    topics = [config.topic for config in consumer_config.configs]
    loop.run_until_complete(consume(topics, queue))

async def consume(topics, queue):
    loop = asyncio.get_event_loop()
    conf = get_kafka_conf()
    start_command_handler()
    consumer = AIOKafkaConsumer(
        *topics,
        loop=loop,
        bootstrap_servers=conf.get('bootstrap.servers'),
        group_id=conf.get('client.id'),
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        while True:
            result = await consumer.getmany(timeout_ms=10 * 1000)
            for tp, messages in result.items():
                if messages:
                    process_msg_batch(messages)
                    # Commit progress only for this partition
                    await consumer.commit({
                        tp: messages[-1].offset + 1
                    })
                    frappe.db.commit()
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()

def process_msg_batch(messages):
    for message in messages:
        frappe.get_doc({
            'doctype': 'Message Log',
            'direction': 'Received',
            'json_message': message.value,
        }).insert(ignore_permissions=True)