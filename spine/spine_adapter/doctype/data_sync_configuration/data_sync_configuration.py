# -*- coding: utf-8 -*-
# Copyright (c) 2019, ElasticRun and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
import json
from frappe.model.document import Document
from frappe.utils.logger import get_logger
from spine.utils.command_controller import publish_command

module_name = __name__
logger = None

def get_module_logger():
	global logger
	logger = get_logger(module_name, with_more_info=False)
	return logger

class DataSyncConfiguration(Document):
	def validate(self):
		# self.validate_local_fields()
		pass

	def on_update(self):
		publish_command('reload_config')

	def validate_local_fields(self):
		meta = frappe.get_meta(self.local_doctype, cached=False)
		fieldnames = set([df.fieldname for df in meta.get("fields")])
		local_fields_map = set([item.local_field for item in self.get('field_map', [])])
		local_matching_fields = set([item.local_field for item in self.get('matching_fields')])
		local_sync_fields = set([item.local_field for item in self.get('data_sync_fields')])
		for field in (list(local_fields_map) + list(local_matching_fields) + list(local_sync_fields)):
			if field not in (fieldnames + ['name']):
				frappe.throw(f"Wrong local_field Added {field}")

#converters = This is used for Converting value
converters = {
	'str': str,
	'float': float,
	'int': int,
}

#Consumer Data Sync
def three_s_sync(message):
	#Add the Remote Doctype and Remote Doc name in message_log
	remote_doctype = message.get('Header').get('DocType')
	remote_docname = message.get('Payload').get('name')
	remote_doc = message.get('Payload')
	remote_doc['__remote_doctype'] = remote_doctype
	remote_doc['__remote_docname'] = remote_docname
	if not remote_doc:
		return

	#Data sync configuration Check in Spine Module
	data_configuration = frappe.db.get_value('Data Sync Configuration', filters={
		'remote_doctype': remote_doctype,
		'is_consumer': 1,
	}, fieldname='name')

	if not data_configuration:
		frappe.throw('Consumer Data Sync Configuration not found for remote doctype {}'.format(remote_doctype))

	local_doc = frappe._dict()
	data_sync_config = frappe.get_cached_doc('Data Sync Configuration', data_configuration)
	local_doctype = data_sync_config.local_doctype or data_sync_config.remote_doctype

	# Part 1: Map
	for row in data_sync_config.field_map:
		set_value(local_doc, row.local_field, remote_doc.get(row.remote_field))

	#Part 2 : Data Sync Field to Sync
	fields_to_sync = [row.local_field for row in data_sync_config.data_sync_fields]
	if fields_to_sync:
		for fieldname in fields_to_sync + [
			'__remote_doctype',
			'__remote_docname',
		] + [row.local_field for row in data_sync_config.matching_fields]:
			if fieldname not in local_doc:
				local_doc[fieldname] = remote_doc.get(fieldname)
	else:
		for field in remote_doc:
			if not field in local_doc:
				# doc[field] = remote_doc[field]
				set_value(local_doc, field, remote_doc[field])

	#Part 3 Value Mapping For Converting Values
	for row in (data_sync_config.data_sync_value_maps or []):
		converter = converters.get(row.fieldtype, str)
		if converter(local_doc.get(row.local_field)) == converter(row.remote_value):
			set_value(local_doc, row.local_field, converter(row.local_value))


	# Check the Links Exist or Not
	lookup_link_name(remote_doc, local_doc)

	# Part 2: Search in Document Map
	mapped_doc = frappe.db.get_value('Document Map', filters={
		'remote_doctype': remote_doctype,
		'remote_name': remote_docname,
	}, fieldname=['local_doctype', 'local_name'], as_dict=True)

	#if Exist in Document Map Update it
	if mapped_doc:
		return update_doc(mapped_doc['local_doctype'], mapped_doc['local_name'], local_doc)

	filters = []
	for field in data_sync_config.matching_fields:
		try:
			tablefield, fieldname = field.local_field.split('.')
		except ValueError:
			fieldname = field.local_field
			tablefield = None

		if tablefield:
			tablename = frappe.get_meta(local_doctype).get_field(tablefield).options
			filter_value = local_doc.get(tablefield)[0].get(fieldname)
			filters.append([tablename, fieldname, '=', filter_value])
		else:
			filter_value = local_doc.get(fieldname)
			filters.append([local_doctype, fieldname, '=', filter_value])
		if type(filter_value) not in (str, int, float):
			frappe.throw(f'Invalid filter type {type(filter_value)} for Filter: {filters[-1]}')
		elif frappe.utils.cint(field.is_reqd) and (type(filter_value) not in (int, float)) and not filter_value:
			frappe.throw(f'Invalid filter value "{filter_value}" for Filter: {filters[-1]}')

	#To Check the Local Doc and Filter Exist then return that doc name
	if not filters:
		local_doc_name = None
	else:
		local_doc_name = frappe.get_all(
			local_doctype,
			filters=filters,
			limit_page_length=10,
			debug=1,
		)

	#Exist Local Doc then Update it.
	if local_doc_name:
		if len(local_doc_name) > 1:
			frappe.throw('Multiple documents found for match filters')
		local_doc_name = local_doc_name[0].name
		return update_doc(local_doctype, local_doc_name, local_doc, add_to_map=True)

	if frappe.utils.cint(data_sync_config.do_not_create):
		frappe.throw(f'''
			Unable to find remote data {remote_doctype} {remote_docname}.
			Will not create new document as config forbids the same.
		''')
	#create New Doc if Not Exist local_doc_name and mapped_doc
	local_doc['doctype'] = data_sync_config.get('local_doctype', remote_doctype)

	local_doc = frappe.get_doc(local_doc)
	local_doc.flags.last_updated_by_spine = True
	local_doc.insert(ignore_permissions=True)
	add_to_doc_map(local_doc)

#To Check links in Remote and Local Doc
def lookup_link_name(remote_doc, local_doc):
	for link in remote_doc.get('links', []):
		remote_doctype = link.get('link_doctype')
		remote_name = link.get('link_name')
		if not (remote_name and remote_doctype):
			continue

		local_link = frappe.db.get_value('Document Map', filters={
			'remote_doctype': remote_doctype,
			'remote_name': remote_name,
		}, fieldname=['local_name as link_name', 'local_doctype as link_doctype'], as_dict=True)
		if not local_link:
			frappe.throw(f'Could not find remote link name {remote_name} for {remote_doctype}')
		local_doc.setdefault('links', []).append(local_link)

#Update the Document map if exist and
def update_doc(doctype, docname, update_dict, add_to_map=False):
	doc = frappe.get_doc(doctype, docname)
	for field in update_dict:
		if field in ['name', 'modified', 'naming_series']:
			continue
		doc.set(field, update_dict[field])
	doc.flags.last_updated_by_spine = True
	doc.save(ignore_permissions=True)
	if add_to_map:
		add_to_doc_map(doc)

def set_value(doc, field, value):
	if isinstance(value, list):
		doc[field] = json.loads(frappe.as_json(value))
	else:
		doc[field] = value

#To Create a new Document Map
def add_to_doc_map(doc):
	doc_map = frappe.get_doc({
		'doctype': 'Document Map',
		'remote_doctype':doc.get('__remote_doctype'),
		'remote_name': doc.get('__remote_docname'),
		'local_doctype': doc.get('doctype'),
		'local_name': doc.name,
	}).insert(ignore_permissions=True)
	add_comment(doc, doc_map)

#Add Comment For New Document Sync
def add_comment(doc, doc_map):
	msg = f'Document map <a href="/desk#Form/Document Map/{doc_map.name}">{doc_map.name}</a> created'
	frappe.get_doc({
		'doctype': "Communication",
		'subject': "Sync",
		'content': msg,
		'communication_type': "Comment",
		'comment_type': "Comment",
		'reference_doctype': doc.get('doctype'),
		'reference_name': doc.name,
	}).insert(ignore_permissions=True)
