import frappe
import asyncio
import json
import os
import sys
from uuid import uuid4
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from spine.utils import get_kafka_conf

def publish_command(command):
    frappe.utils.background_jobs.enqueue(
        _publish_command,
        command=command,
        enqueue_after_commit=True,
    )

def _publish_command(command):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(produce_command_message(command))

async def produce_command_message(command):
    loop = asyncio.get_event_loop()
    conf = get_kafka_conf()
    producer = AIOKafkaProducer(
        loop=loop,
        bootstrap_servers=conf.get('bootstrap.servers')
    )
    await producer.start()
    message = {
        'Header': {
            'Command': command,
        }
    }
    try:
        await producer.send_and_wait(
            f"{conf.get('client.id')}_command_handler",
            json.dumps(message).encode()
        )
    finally:
        await producer.stop()

def start_command_handler():
    loop = asyncio.get_event_loop()
    loop.create_task(handle_commands())

async def handle_commands():
    conf = get_kafka_conf()
    loop = asyncio.get_event_loop()
    consumer = AIOKafkaConsumer(
        f"{conf.get('client.id')}_command_handler",
        loop=loop,
        bootstrap_servers=conf.get('bootstrap.servers'),
        group_id=str(uuid4()),
        auto_offset_reset='latest',
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        async for message in consumer:
            try:
                handle_command(message)
            except Exception:
                frappe.log_error(title='Error while handling kafka command')
    finally:
        await consumer.stop()

def handle_command(message):
    message = json.loads(message.value)
    command = message.get('Header', {}).get('Command')
    if command and hasattr(CommandHandler, command):
        getattr(CommandHandler, command)()

class CommandHandler(object):
    @staticmethod
    def reload_config():
        frappe.logger().debug('Reload config called, restarting self')
        restart_self()

def restart_self():
    try:
        p = psutil.Process(os.getpid())
        for handler in p.get_open_files() + p.connections():
            os.close(handler.fd)
    except:
        pass

    python = sys.executable
    os.execl(python, python, *sys.argv)