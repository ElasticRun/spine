import frappe

def purge_to_file(payload):
    """
    Purging all Payload to FS
    """
    file = open(frappe.utils.get_files_path() + '/purge_msgs.{}.txt'.format(str(frappe.utils.data.get_datetime().date())), '+a') 
    file.write(str(payload) + "\n\n")
    file.close()