# -*- coding: utf-8 -*-
from __future__ import unicode_literals

app_name = "spine"
app_title = "Spine Adapter"
app_publisher = "ElasticRun"
app_description = "Spine Adapter"
app_icon = "octicon octicon-file-directory"
app_color = "grey"
app_email = "engineering@elastic.run"
app_license = "MIT"

# Includes in <head>
# ------------------

# include js, css files in header of desk.html
# app_include_css = "/assets/spine/css/spine.css"
# app_include_js = "/assets/spine/js/spine.js"

# include js, css files in header of web template
# web_include_css = "/assets/spine/css/spine.css"
# web_include_js = "/assets/spine/js/spine.js"

# include js in page
# page_js = {"page" : "public/js/file.js"}

# include js in doctype views
# doctype_js = {"doctype" : "public/js/doctype.js"}
# doctype_list_js = {"doctype" : "public/js/doctype_list.js"}
# doctype_tree_js = {"doctype" : "public/js/doctype_tree.js"}
# doctype_calendar_js = {"doctype" : "public/js/doctype_calendar.js"}

# Home Pages
# ----------

# application home page (will override Website Settings)
# home_page = "login"

# website user home page (by Role)
# role_home_page = {
#	"Role": "home_page"
# }

# Website user home page (by function)
# get_website_user_home_page = "spine.utils.get_home_page"

# Generators
# ----------

# automatically create page for each record of this doctype
# website_generators = ["Web Page"]

# Installation
# ------------

# before_install = "spine.install.before_install"
# after_install = "spine.install.after_install"

# Desk Notifications
# ------------------
# See frappe.core.notifications.get_notification_config

# notification_config = "spine.notifications.get_notification_config"

# Permissions
# -----------
# Permissions evaluated in scripted ways

# permission_query_conditions = {
# 	"Event": "frappe.desk.doctype.event.event.get_permission_query_conditions",
# }
#
# has_permission = {
# 	"Event": "frappe.desk.doctype.event.event.has_permission",
# }

# Document Events
# ---------------
# Hook on document methods and events

# doc_events = {
# 	"*": {
# 		"on_update": "method",
# 		"on_cancel": "method",
# 		"on_trash": "method"
#	}
# }

doc_events = {
    "SpineProducerConfig": {
        "on_update": "spine.spine_adapter.docevents.eventhandler.update_config_hooks"
    },
    "SpineConsumerConfig": {
        "on_update": "spine.spine_adapter.docevents.eventhandler.update_config_hooks"
    },
    "*": {
        "on_update": "spine.spine_adapter.docevents.eventhandler.handle_event",
        "on_update_after_submit": "spine.spine_adapter.docevents.eventhandler.handle_event",
        "after_rename": "spine.spine_adapter.docevents.eventhandler.handle_event",
        "on_trash": "spine.spine_adapter.doctype.document_map.document_map.cascade_remove_document_maps",
        "on_submit": "spine.spine_adapter.docevents.eventhandler.handle_event",
        "on_cancel": "spine.spine_adapter.docevents.eventhandler.handle_event",
    }
}

# Scheduled Tasks
# ---------------

scheduler_events = {
    "cron": {
        "*/2 * * * *": [
            # job to look for new messages and process them when received - used for spine consumer side. Runs every 2 mins.
            "spine.spine_adapter.scheduler.message_processor.poll_and_process_new_messages",
            # job to look for error messages and process them - used for spine consumer side. Runs every 2 min.
            "spine.spine_adapter.scheduler.error_message_processor.poll_and_process_error_messages",
            # job to look for new messages and publish them to kafka - used on spine producer side. Runs every 2 mins.
            "spine.spine_adapter.scheduler.message_processor.poll_and_publish_new_messages"
        ]
    }
}
# scheduler_events = {
# 	"all": [
# 		"spine.tasks.all"
# 	],
# 	"daily": [
# 		"spine.tasks.daily"
# 	],
# 	"hourly": [
# 		"spine.tasks.hourly"
# 	],
# 	"weekly": [
# 		"spine.tasks.weekly"
# 	]
# 	"monthly": [
# 		"spine.tasks.monthly"
# 	]
# }

# Testing
# -------

# before_tests = "spine.install.before_tests"

# Overriding Whitelisted Methods
# ------------------------------
#
# override_whitelisted_methods = {
# 	"frappe.desk.doctype.event.event.get_events": "spine.event.get_events"
# }
