import copy
import unittest

import cchain

from . import mixins
from . import samples


class BaseChangesProcessorTestCase(
    unittest.TestCase,
    mixins.ProcessChangesTestMixin
):

    def setUp(self):

        self.processor = cchain.processors.base.BaseChangesProcessor()
        self.samples = samples.CHANGES

    def test_process_change_line(self):
        change_line = self.samples[-1]
        expected_result = (
            change_line,
            change_line['changes'][0]['rev'],
            change_line['seq']
        )

        processed_change = self.processor.process_change_line(change_line)
        self.assertEqual(processed_change, expected_result)


class BaseDocChangesProcessorTestCase(
    unittest.TestCase,
    mixins.ProcessChangesTestMixin
):

    def setUp(self):

        self.processor = cchain.processors.base.BaseDocChangesProcessor(
            seq_property=None
        )
        self.samples = samples.CHANGES_DOCS

    def test_process_change_line(self):
        change_line = self.samples[-1]
        expected_result = (
            change_line['doc'],
            change_line['changes'][0]['rev'],
            change_line['seq']
        )

        processed_change = self.processor.process_change_line(change_line)
        self.assertEqual(processed_change, expected_result)


class BaseDocWithSeqChangesProcessorTestCase(
    unittest.TestCase,
    mixins.ProcessChangesTestMixin
):

    def setUp(self):

        self.processor = cchain.processors.base.BaseDocChangesProcessor()
        self.samples = samples.CHANGES_DOCS

    def test_process_change_line(self):
        change_line = self.samples[-1]
        expected_doc = copy.deepcopy(change_line['doc'])
        expected_doc['_seq'] = change_line['seq']

        expected_result = (
            expected_doc,
            change_line['changes'][0]['rev'],
            change_line['seq']
        )

        processed_change = self.processor.process_change_line(change_line)
        self.assertEqual(processed_change, expected_result)
