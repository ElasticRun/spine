import frappe

def log_error(doc, event):
	print('--- Printing Document ---')
	print(doc.__dict__)
