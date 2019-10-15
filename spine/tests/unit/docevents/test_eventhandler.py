import datetime, logging, unittest
import frappe

from unittest.mock import patch

from spine.spine_adapter.docevents.eventhandler import *


def mock_get_logger(loggername, with_more_info=False):
    logger = logging.getLogger(loggername)
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)
    return logger


class TestEventHandlerUpsertDocument(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @patch('frappe.get_doc')
    @patch('spine.spine_adapter.docevents.eventhandler.get_logger')
    @patch('spine.spine_adapter.kafka_client.kafka_producer.get_logger')
    def test_new_document(self, get_logger_method2, get_logger_method, get_doc_method ):
        object = {"test_attr1": "test_value1", "test_attr2": "test_value2"}
        input_doctype = "TestDoctype"
        doc = {"Header": {"Origin": "unittest-client",
                         "Timestamp": datetime.datetime.now(),
                         "DocType": input_doctype,
                         "Event": "on_update",
                         "Topic": "test_topic"},
               "Payload": object }
        event = "on_update"

        #setup Mocks
        get_doc_results = mock_get_doc_nonexistent(input_doctype, object)
        get_doc_method.side_effect = get_doc_results
        #insert_method.return_value = mock_insert_doc(input_doctype, object)
        get_logger_method.return_value = mock_get_logger("test")
        get_logger_method2.return_value = mock_get_logger("test")

        #Call test method
        upsert_document(doc, event)

        # Assertions to check if behaviour of the code was as expected.
        self.assertTrue(get_doc_method.called, "frappe.get_doc method was not called as expected")
        #self.assertTrue(insert_method.called, "Insert method was not called as expected")
        self.assertEqual(2, get_doc_method.call_count, "frappe.get_doc method was not called expected no. of times")
        get_doc_method.assert_any_call("DocType", input_doctype)

        assert_result = {"doctype": input_doctype }
        assert_result.update(object)
        get_doc_method.assert_any_call(assert_result)
        self.assertEqual(1, get_doc_results[1].insert_called_counter, "Insert method was not called as expected")
        self.assertEqual(0, get_doc_results[1].save_called_counter, "Save method was called when not expected")


    @patch('frappe.get_doc')
    @patch('spine.spine_adapter.docevents.eventhandler.get_logger')
    @patch('spine.spine_adapter.kafka_client.kafka_producer.get_logger')
    def test_existing_document(self, get_logger_method2, get_logger_method, get_doc_method ):
        data = {"name": "test-result", "test_attr1": "test_value1", "test_attr2": "test_value2"}
        input_doctype = "TestDoctype"
        doc = {"Header": {"Origin": "unittest-client",
                         "Timestamp": datetime.datetime.now(),
                         "DocType": input_doctype,
                         "Event": "on_update",
                         "Topic": "test_topic"},
               "Payload": data }
        event = "on_update"

        #setup Mocks
        get_doc_results = mock_get_doc_existent(input_doctype, data)
        get_doc_method.side_effect = get_doc_results
        #insert_method.return_value = mock_insert_doc(input_doctype, object)
        get_logger_method.return_value = mock_get_logger("test")
        get_logger_method2.return_value = mock_get_logger("test")

        #Call test method
        upsert_document(doc, event)

        # Assertions to check if behaviour of the code was as expected.
        self.assertTrue(get_doc_method.called, "frappe.get_doc method was not called as expected")
        #self.assertTrue(insert_method.called, "Insert method was not called as expected")
        self.assertEqual(2, get_doc_method.call_count, "frappe.get_doc method was not called expected no. of times")
        get_doc_method.assert_any_call("DocType", input_doctype)

        get_doc_method.assert_any_call({"doctype" : input_doctype, "name": "test-result"})
        self.assertEqual(0, get_doc_results[1].insert_called_counter, "Insert method was called when not expected")
        self.assertEqual(1, get_doc_results[1].save_called_counter, "Save method was not called as expected")

    @patch('frappe.get_doc')
    @patch('spine.spine_adapter.docevents.eventhandler.get_logger')
    @patch('spine.spine_adapter.kafka_client.kafka_producer.get_logger')
    def test_nonexistent_document_withname(self, get_logger_method2, get_logger_method, get_doc_method):
        data = {"name": "test-result", "test_attr1": "test_value1", "test_attr2": "test_value2"}
        input_doctype = "TestDoctype"
        doc = {"Header": {"Origin": "unittest-client",
                          "Timestamp": datetime.datetime.now(),
                          "DocType": input_doctype,
                          "Event": "on_update",
                          "Topic": "test_topic"},
               "Payload": data}
        event = "on_update"

        # setup Mocks
        get_doc_results = mock_get_doc_nonexistent_withname(input_doctype, data)
        get_doc_method.side_effect = get_doc_results
        # insert_method.return_value = mock_insert_doc(input_doctype, object)
        get_logger_method.return_value = mock_get_logger("test")
        get_logger_method2.return_value = mock_get_logger("test")

        # Call test method
        upsert_document(doc, event)

        # Assertions to check if behaviour of the code was as expected.
        self.assertTrue(get_doc_method.called, "frappe.get_doc method was not called as expected")
        # self.assertTrue(insert_method.called, "Insert method was not called as expected")
        self.assertEqual(2, get_doc_method.call_count, "frappe.get_doc method was not called expected no. of times")
        get_doc_method.assert_any_call("DocType", input_doctype)

        get_doc_method.assert_any_call({"doctype": input_doctype, "name": "test-result"})
        self.assertEqual(1, get_doc_results[1].insert_called_counter, "Insert method was not called as expected")
        self.assertEqual(1, get_doc_results[1].save_called_counter, "Save method was not called as expected")


    # def test_cleanup_string_with_dots(self):
    #     input = "Test.String.Input"
    #     expected = "Test String Input"
    #     result = cleanup_string(input)
    #     self.assertEqual(expected, result, msg="cleanup_string() Method did not return expected result.")
    #
    # def test_cleanup_string_with_dots_no_title(self):
    #     input = "tesT.stRing.inpUt"
    #     expected = "tesT stRing inpUt"
    #     result = cleanup_string(input)
    #     self.assertEqual(expected, result, msg="cleanup_string() Method did not return expected result.")
    #
    # def test_cleanup_string_without_dots(self):
    #     input = "TestString Input"
    #     expected = "TestString Input"
    #     result = cleanup_string(input)
    #     self.assertEqual(expected, result, msg="cleanup_string() Method did not return expected result.")
    #
    # def test_cleanup_string_with_dot_in_end(self):
    #     input = "TestString Input."
    #     expected = "TestString Input"
    #     result = cleanup_string(input)
    #     self.assertEqual(expected, result, msg="cleanup_string() Method did not return expected result.")
    #
    # def test_cleanup_string_with_multiple_dots(self):
    #     input = "Test..String..Input"
    #     expected = "Test String Input"
    #     result = cleanup_string(input)
    #     self.assertEqual(expected, result, msg="cleanup_string() Method did not return expected result.")
    #
    # def test_cleanup_string_with_multiple_dots_in_end(self):
    #     input = "Test..String..Input.."
    #     expected = "Test String Input"
    #     result = cleanup_string(input)
    #     self.assertEqual(expected, result, msg="cleanup_string() Method did not return expected result.")




def mock_get_doc_nonexistent_withname(input_doctype, data):
    # First call should return the input doctype as name with fields.
    # Second call should return the input doctype as doctype along with actual object.
    first_result = frappe._dict()
    first_result.name = input_doctype
    first_result.doctype = "DocType"
    first_result.fields = []
    for key in data.keys():
        first_result.fields.append(frappe._dict().update({"fieldname": key}))
    first_result.fields.append(frappe._dict().update({"fieldname": "name"}))

    second_result = MockDocument({"doctype": input_doctype, "name": "test-result"})
    second_result.update(data)
    second_result.set("__islocal", True)
    return [first_result,
            second_result]


def  mock_get_doc_existent(input_doctype, data):
    # First call should return the input doctype as name with fields.
    # Second call should return the input doctype as doctype along with actual object.
    first_result = frappe._dict()
    first_result.name = input_doctype
    first_result.doctype = "DocType"
    first_result.fields = []
    for key in data.keys():
        first_result.fields.append(frappe._dict().update({"fieldname": key}))
    first_result.fields.append(frappe._dict().update({"fieldname": "name"}))

    second_result = MockDocument({"doctype": input_doctype, "name": "test-result"})
    second_result.update(data)
    return [first_result,
            second_result]


def mock_get_doc_nonexistent(input_doctype, data):
    # First call should return the input doctype as name with fields.
    # Second call should return the input doctype as doctype along with actual object.
    first_result = frappe._dict()
    first_result.name = input_doctype
    first_result.doctype= "DocType"
    first_result.fields = []
    for key in data.keys():
        first_result.fields.append(frappe._dict().update({"fieldname": key}))

    second_result = MockDocument({"doctype": input_doctype, "name": "test-result"})
    second_result.update(data)
    return [first_result,
            second_result]


# def mock_insert_doc(input_doctype, object):
#     result = frappe._dict().update({"name": "test-result",
#             "doctype": input_doctype})
#     result.update(object)
#
#     return result


class MockDocument(frappe.model.document.Document):
    insert_called_counter = 0
    save_called_counter = 0
    args = {}
    def __init__(self, *args, **kwargs):
        super(MockDocument, self).__init__(*args, **kwargs)
        self.args.update(*args)

    def load_from_db(self):
        pass

    def insert(self):
        self.insert_called_counter = self.insert_called_counter + 1

    def save(self):
        self.save_called_counter = self.save_called_counter + 1
        if hasattr(self, "__islocal"):
            self.insert()

    def init_valid_columns(self):
        pass
