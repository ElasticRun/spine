# -*- coding: utf-8 -*-
# Copyright (c) 2019, ElasticRun and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
from frappe.model.document import Document
from spine.utils.indexing import create_index

class DocumentMap(Document):
	pass

def on_doctype_update():
	create_index('tabDocument Map', ['remote_doctype', 'remote_name'], unique=True)
	create_index('tabDocument Map', ['local_doctype', 'local_name'], unique=True)

def cascade_remove_document_maps(doc, event=None):
	doc_map = frappe.db.get_value('Document Map', filters={
		'local_doctype': doc.doctype,
		'local_name': doc.name,
	}, fieldname='name')
	if doc_map:
		frappe.delete_doc('Document Map', doc_map, force=True)