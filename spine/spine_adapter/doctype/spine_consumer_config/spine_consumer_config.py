# -*- coding: utf-8 -*-
# Copyright (c) 2019, ElasticRun and contributors
# For license information, please see license.txt

from __future__ import unicode_literals

from frappe.model.document import Document
from spine.utils.command_controller import publish_command

class SpineConsumerConfig(Document):
    def on_update(self):
        publish_command('reload_config')
