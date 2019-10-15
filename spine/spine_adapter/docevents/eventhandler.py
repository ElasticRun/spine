import re
import traceback

import frappe
import json
from frappe.model.document import Document
from frappe.utils.logger import get_logger

from spine.spine_adapter.kafka_client.kafka_producer import publish_doc_event

module_name = __name__

class HandlerNotDefined(Exception):
    pass

def get_module_logger():
    return get_logger(module_name, with_more_info=False)

#
# doctype_to_flag_mapping = {
#     "Purchase Order": "enable_po",
#     "Purchase Invoice": "enable_pi",
#     "Sales Order": "enable_so",
#     "Sales Invoice": "enable_si",
#     "Item": "enable_item"
# }
#
# doctype_to_topic_mapping = {
#     "Purchase Order": "topic_po",
#     "Purchase Invoice": "topic_pi",
#     "Sales Order": "topic_so",
#     "Sales Invoice": "topic_si",
#     "Item": "topic_item"
# }
#
# doctype_to_handler_mapping = {
#     "Purchase Order": "action_po",
#     "Purchase Invoice": "action_pi",
#     "Sales Order": "action_so",
#     "Sales Invoice": "action_si",
#     "Item": "action_item"
# }


# def init_consumer():
#     if not KafkaAsyncSingletonConsumer.is_initialized():
#         logger.debug("Initializing Kafka Consumer")
#         KafkaAsyncSingletonConsumer()


def update_config_hooks(doc, event):
    logger = get_module_logger()
    # This is called when user updates the single document SpineConsumerConfig
    # Clear cached version so that new calls will get updated data.
    doctype_updated = type(doc).__name__
    logger.debug("Clearing {} Cache.".format(doctype_updated))
    frappe.clear_document_cache(doctype_updated, doctype_updated)
    logger.debug("{} Cache cleared.".format(doctype_updated))
    logger.debug("Updating new values in cache")
    cache_key = frappe.get_document_cache_key(doctype_updated, doctype_updated)
    cache_conn = frappe.cache()
    cache_conn.hset('document_cache', cache_key, doc.as_dict())
    logger.debug("New values saved to document_cache")


def handle_event(doc, event, *args):
    if not frappe.conf.developer_mode:
        frappe.utils.background_jobs.enqueue(
            publish_event,
            doc=doc,
            docevent=event,
            extra_args=args,
            enqueue_after_commit=True,
        )
    else:
        handle_event_wrapped(doc, event, *args)

def publish_event(doc, docevent, extra_args):
    handle_event_wrapped(doc, docevent, *extra_args)

# handler method for all events on all doc types.
def handle_event_wrapped(doc, event, *args):
    logger = get_module_logger()
    if not frappe.local.conf.get("kafka"):
        logger.debug("No Kafka configuration found in site config")
        return

    logger.debug("Document received - {}. Event received - {}".format(doc, event))

    # config = frappe.get_single("Spine Producer Config").as_dict()
    config = frappe.get_cached_doc("Spine Producer Config", "Spine Producer Config").as_dict()
    doctype_updated = doc.get("doctype")
    # doctype_updated = type(doc).__name__
    # doctype_updated = doctype
    # producer_handler, target_topic, found, retry_count, retry_interval = get_producer_handler_for_doctype(config, doctype_updated)
    handlers = [row for row in config.configs if row.document_type == doctype_updated]
    debug_flag = config.get("message_debug")
    # logger.debug("enabled - {} and topic - {}".format(producer_handler, target_topic))
    if not handlers:
        logger.debug("Event handling for doctype {} is not enabled. Ignoring the event {}".format(doctype_updated, event))
        return

    logger.debug("Producer is enabled for {}".format(doctype_updated))
    # target_topic = "events.topic"
    # Defaulting to 'events.topic' in case no Topic name was configured.

    ## forming a dict with all the params (doc_to_publish, doctype_updated, target_topic, event, retry_count, retry_interval)
    ## This is a way to provide hook to the handler for overwriting the configs
    for producer_handler in handlers:
        topic = producer_handler.get('topic', 'events.topic')
        logger.debug("Target Topic for doctype {} is configured for topic {}".format(doctype_updated, topic))
        try:
            producer_config_dict = {
                'doc_to_publish': doc,
                'args': args,
                'doctype_updated': doctype_updated,
                'target_topic': topic,
                'event': event,
                'retry_count': producer_handler.retry_count,
                'retry_interval': producer_handler.retry_interval,
            }
            event_handler_name = (producer_handler.event_handler or '').strip()
            if not event_handler_name:
                raise HandlerNotDefined

            handler_func = frappe.get_attr(event_handler_name)
            handler_response = handler_func(producer_config_dict)
            if not handler_response:
                logger.debug("Handler function did not return any data. Skipping the update event.")
                logger.debug("Document was filtered out by handler. Event Ignored.")
                continue
            if isinstance(handler_response, Document):
                producer_config_dict['doc_to_publish'] = handler_response
            elif type(handler_response) is dict:
                if not set(handler_response.keys()) >= set(['doc_to_publish', 'doctype_updated', 'target_topic', 'event', 'retry_count', 'retry_interval']):
                    logger.debug("Handler function not returning all the keys of dict producer config.")
                    ## if the keys do not match, here is an assumption that only Document is sent as dict.
                    producer_config_dict['doc_to_publish'] = handler_response
                else:
                    producer_config_dict = handler_response
            elif type(handler_response) is str:
                frappe.log_error("Handler function returning document in string not dict")
                producer_config_dict['doc_to_publish'] = json.loads(handler_response)
            else:
                raise Exception("Handler returning incorrect response.")
            logger.debug("Handler function returned updated document as - {}".format(
                producer_config_dict.get('doc_to_publish')))
        except HandlerNotDefined:
            producer_config_dict['doc_to_publish'] = doc
        except:
            frappe.log_error("Could not load/execute producer handler function {}. Exception - {}".format(
                producer_handler.event_handler, traceback.format_exc()), "Producer handler Failure")
            logger.error("Could not load/execute producer handler function {}.".format(producer_handler))
            raise
        try:
            publish_doc_event(
                doc=producer_config_dict.get('doc_to_publish'),
                doctype=producer_config_dict.get('doctype_updated'),
                target_topic=producer_config_dict.get('target_topic'),
                event=producer_config_dict.get('event'),
                debug_flag=debug_flag,
                retry_count=producer_config_dict.get('retry_count'),
                retry_interval=producer_config_dict.get('retry_interval'),
                args=args,
            )
            logger.debug("Event published.")
        except:
            frappe.log_error(title="Could not publish message to spine")
            raise

#
# def consume_doc_events():
#     topics, handlers = get_enabled_topics_handlers()
#     logger.debug("Topics to subscribe to - {}".format(topics))
#     logger.debug("Handlers - {}".format(frappe.as_json(handlers)))
#
#     # Singleton consumer that works asynchronously and keeps processing messages until stopped.
#     init_consumer()
#     logger.debug("Starting Consumer")
#     KafkaAsyncSingletonConsumer.start(topics, handlers)
#     logger.debug("Consumer Started")

# def upsert_document(doc, event=None):
#     """
#     In-built handler provided for Spine adapter. This handler can be configured for any doctype being consumed by spine
#     adapter. The handler method tries to insert the new record if it does not exist, and updates an existing record, if
#     a record with same 'name' is found.
#
#     The handler also checks if the incoming doctype and existing doctype have any differences in the structure. If the
#     new doctype has additional columns, they are automatically added to the local doctype before saving the incoming
#     record.
#
#     Example Usage : Add "spine.spine_adapter.docevents.eventhandler.upsert_document" as the event handler for the
#     DocType that needs to be synced.
#
#     Note: missing columns/fields are not deleted from local doctype, for obvious reasons. However, if such missing
#     fields are marked as mandatory, the local save will fail.
#
#     :param doc: Incoming document containing the header information along with the actual payload.
#     :param event: The original event that generated the incoming document.
#     :return: None
#     """
#     # set because it takes only unique name and provides difference functionality
#     # new_field_names = set([1,2,3,4,5,1,1,4])
#     # new_field_names = {1,2,3,4,5}
#     # doctype_fields = {1,2,3}
#     # final_list = new_field_names - doctype_fields
#     # output: final_list = {4,5}
#     # First check if the incoming fields and local fields match. If not, add any new fields from incoming messages.
#     global logger
#     get_module_logger()
#     incoming_doctype = doc.get("Header").get("DocType")
#     logger.debug("Incoming DocType - {}".format(incoming_doctype))
#
#     module_def = get_or_create_module_definition(doc)
#     module_name = module_def.get("module_name")
#     logger.debug("Module Name - {}".format(module_name))
#     try:
#         local_doctype = frappe.get_doc("DocType", incoming_doctype)
#     except frappe.exceptions.DoesNotExistError:
#         # incoming DocType does not exist. Create it first.
#         local_doctype = frappe.get_doc({ "doctype": "DocType", "name": incoming_doctype,
#                                          "module": module_name, "fields" : [{
#                                         'label': "Name",
#                                         'fieldtype': 'Data',
#                                         'fieldname': "name",
#                                         'read_only': 1,
#                                         'in_list_view': 1,
#                                         'autoname': "hash",
#                                         'track_changes': 0,
#                                         'in_create': 1,
#                                         '__islocal': 1,
#                                     }] })
#         local_doctype.append('permissions', {
#                 'role': 'System Manager',
#                 'read': 1,
#                 'write': 1,
#                 'create': 1,
#                 'report': 1,
#                 'export': 1,
#             })
#         local_doctype.append('fields', {
# 				'label': "Synced Date",
# 				'in_list_view': 1,
# 				'fieldtype': 'Date',
# 				'fieldname': '__date',
# 				'read_only': 1})
#         local_doctype = local_doctype.insert()
#         logger.debug("New DocType created - {}".format(local_doctype.as_dict()))
#
#     local_fields = set([field.fieldname for field in local_doctype.fields])
#     logger.debug("Local Fields - {}".format(local_fields))
#     # for field in local_doctype.fields:
#     #     local_fields.update(field.fieldname)
#     new_fields = set([key for key in doc.get("Payload").keys()])
#     logger.debug("New Fields - {}".format(new_fields))
#     additional_fields = new_fields - local_fields
#     logger.debug("Additional Fields - {}".format(additional_fields))
#     if additional_fields:
#         # Process additional fields.
#         for field in additional_fields:
#             local_doctype.append('fields', {
#                 'label': frappe.unscrub(field),
#                 'fieldtype': 'Data',
#                 'fieldname': remove_special_char(field),
#                 'read_only': 1,
#                 'in_list_view': 1
#             })
#         logger.debug("Local Doctype after updates - {}".format(local_doctype))
#         local_doctype.save(ignore_permissions=True)
#         frappe.db.commit()
#     # Save the data.
#     doc_to_update = doc.get("Payload")
#     if "name" in new_fields:
#         #Document might already exist. Check and update.
#         try:
#             local_doc = frappe.get_doc(incoming_doctype, doc_to_update.get("name"))
#             logger.debug("Local Doc = {}".format(local_doc.as_dict()))
#             for field in new_fields:
#                 local_doc.set(field, doc_to_update.get(field))
#             logger.debug("Updated existing local doc - {}".format(local_doc.as_dict()))
#             local_doc.save()
#             logger.debug("Saved existing local doc")
#         except frappe.exceptions.DoesNotExistError:
#             # Doc does not exist. Insert new.
#             insert_doc(incoming_doctype, doc_to_update)
#     else:
#         insert_doc(incoming_doctype, doc_to_update)

#
# def insert_doc(incoming_doctype, doc_to_update):
#     doc_to_update.update({"doctype": incoming_doctype})
#     doc_from_frappe = frappe.get_doc(doc_to_update)
#     doc_from_frappe.insert()
#     frappe.db.commit()
#     logger.debug("New doc of {} doctype created.".format(incoming_doctype))


# def remove_special_char(name):
#     # columns in report are generally in the form column_name:Datatype:width
#     # so "column_name:Datatype:width".split(':')[0] will return column_name
#     name = name.split(':')[0]
#     name = name.replace(' ', '_').strip().lower()
#
#     # re.sub replace all the match with ''
#     name = re.sub("[\W]", '', name, re.UNICODE)
#     return name


# def get_or_create_module_definition(msg):
#     header =  msg.get("Header")
#     upsert_config = frappe.get_doc("Spine Upsert Configuration", "Spine Upsert Configuration")
#     app_name = upsert_config.get("app_name")
#     module_name = upsert_config.get("module_name")
#     if (validate_app_and_module(app_name, module_name)):
#         try:
#             module_def = frappe.get_doc("Module Def", module_name)
#         except frappe.exceptions.DoesNotExistError:
#             module_def = frappe.new_doc("Module Def")
#             module_def.app_name = app_name
#             module_def.module_name = module_name
#             logger.debug("Saving module {}".format(module_def.as_dict()))
#             module_def.save(ignore_permissions=True)
#             logger.debug("Saved module {}".format(module_def))
#     else:
#         frappe.throw(_("{0} {1} not found").format(_(app_name), _(module_name) ),
#                      frappe.DoesNotExistError)
#     return module_def


# def validate_app_and_module(app, module):
#     result = False
#     installed_apps = frappe.get_installed_apps()
#     if app in installed_apps:
#         result = True
#     return result

#
# def cleanup_string(str):
#     result = str.replace(".", " ")
#     result = re.sub(' +', ' ', result).strip()
#     return result
