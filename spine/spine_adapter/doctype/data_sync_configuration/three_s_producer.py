import frappe
import json
from frappe.utils.logger import get_logger

module_name = __name__

def get_module_logger():
    return get_logger(module_name, with_more_info=False)

def produce_three_s(producer_dict):
    logger = get_module_logger()
    logger.debug("Payload of Doctype:{0}  Event :{1}".format(producer_dict['doctype_updated'], producer_dict['event']))
    doctype = producer_dict['doctype_updated']
    old_local_doc = producer_dict['doc_to_publish']
    local_doc = frappe.copy_doc(old_local_doc)
    local_doc.name = old_local_doc.name
    local_doc.run_method('before_produce')

    remote_doc = frappe._dict()

    #Data sync configuration Check in Spine Module
    data_configuration = frappe.db.get_value('Data Sync Configuration',filters={
        'local_doctype': doctype,
        'is_producer': 1,
	}, fieldname=['name'])

    #This Publish the Default Paylod
    if not data_configuration:
        producer_dict['doc_to_publish'] = local_doc
        frappe.throw('Producer Data Sync Configuration not found for local doctype {}'.format(doctype))

    data_sync_config = frappe.get_cached_doc('Data Sync Configuration', data_configuration)

    #check Flags if not to required consume message
    if old_local_doc.flags.last_updated_by_spine and frappe.utils.cint(data_sync_config.omit_spine_events):
        return

    #To Check the synch Fields
    if data_sync_config.data_sync_fields:
        for field in data_sync_config.data_sync_fields:
            if '.' in field.local_field:
                table_field, fieldname = field.local_field.split('.')
                local_doc_childtable = local_doc.get(table_field, [])
                publish_childtable = remote_doc.setdefault(table_field, [])
                for idx, row in enumerate(local_doc_childtable):
                    if idx >= len(publish_childtable):
                        publish_childtable.append(frappe._dict())
                    publish_childtable[idx][fieldname] = row.get(fieldname)
            else:
                remote_doc[field.local_field] = local_doc.get(field.local_field)

    #To Check the Mapping Field
    if data_sync_config.field_map:
        for row in data_sync_config.field_map:
            remote_doc[row.remote_field] = local_doc.get(row.local_field)

    for row in data_sync_config.data_sync_value_maps:
        force_convert = frappe.utils.cint(row.force_convert)
        evaluate_jinja = frappe.utils.cint(row.evaluate_jinja)
        if '.' in row.local_field:
            table_field, fieldname = row.local_field.split('.')
            for idx, remote_doc_row in enumerate(remote_doc.get(table_field, [])):
                convert_value_map(
                    remote_doc,
                    remote_doc_row,
                    row.local_value,
                    row.remote_value,
                    fieldname,
                    evaluate_jinja,
                    force_convert,
                )
        else:
            convert_value_map(
                remote_doc,
                remote_doc,
                row.local_value,
                row.remote_value,
                row.local_field,
                evaluate_jinja,
                force_convert
            )

    producer_dict['doc_to_publish'] = remote_doc

    logger.debug("Payload Added for Produce Message.")
    return producer_dict

def convert_value_map(remote_doc, remote_doc_row, local_value, remote_value, fieldname, evaluate_jinja, force_convert):
    if evaluate_jinja:
        context = {
            'remote_doc': remote_doc,
            'remote_doc_row': remote_doc_row,
            'get_docmap': get_docmap,
        }
        local_value = frappe.render_template(local_value, context)
        remote_value = frappe.render_template(remote_value, context)
    else:
        local_value = local_value
        remote_value = remote_value
    if force_convert:
        remote_doc_row[fieldname] = remote_value
    elif local_value == remote_doc_row[fieldname]:
        remote_doc_row[fieldname] = remote_value

def get_docmap(dt, dn, remote_or_local, field="name"):
    if remote_or_local == 'remote':
        fetch_field = 'local'
    else:
        fetch_field = 'remote'
        remote_or_local = 'local'

    retval = frappe.db.get_value(
        'Document Map',
        filters={
            f'{fetch_field}_doctype': dt,
            f'{fetch_field}_name': dn,
        },
        fieldname=f'{remote_or_local}_{field}',
    )
    if not retval:
        frappe.throw(f'No remote mapping found for {remote_or_local} {dt} {dn} in document map')
    return retval