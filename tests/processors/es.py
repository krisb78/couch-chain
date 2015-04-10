import unittest

import cchain
import mock

from . import mixins
from . import samples


class SimpleESChangesProcessorTestCase(
    unittest.TestCase,
    mixins.ProcessChangesTestMixin
):
    def setUp(self):
        self.processor = (
            cchain.processors.es.SimpleESChangesProcessor(
                ['http://localhost:5984'],
                'test_index',
                'test_type'
            )
        )
        self.samples = samples.CHANGES_DOCS

        self.expected_bulk_ops = []

        for sample in self.samples:
            doc, rev, seq = self.processor.process_change_line(sample)
            bulk_ops = self.processor.get_ops_for_bulk(doc)
            self.expected_bulk_ops += bulk_ops

    def test_get_ops_for_bulk_change(self):
        change = self.samples[0]

        doc, rev, seq = self.processor.process_change_line(change)
        bulk_ops = self.processor.get_ops_for_bulk(doc)

        expected_bulk_ops = [
            {
                'update': {
                    '_retry_on_conflict': 3,
                    '_type': 'test_type',
                    '_id': '6478c2ae800dfc387396d14e1fc39626',
                    '_index': 'test_index'
                }
            },
            {
                'doc': {
                    '_seq': 6,
                    '_rev': '2-7051cbe5c8faecd085a3fa619e6e6337',
                    '_id': '6478c2ae800dfc387396d14e1fc39626'
                },
                'doc_as_upsert': True
            }
        ]

        self.assertEqual(bulk_ops, expected_bulk_ops)

    def test_get_ops_for_bulk_deletion(self):
        change = self.samples[1]

        doc, rev, seq = self.processor.process_change_line(change)
        bulk_ops = self.processor.get_ops_for_bulk(doc)

        expected_bulk_ops = [
            {
                'delete': {
                    '_type': 'test_type',
                    '_id': '5bbc9ca465f1b0fcd62362168a7c8831',
                    '_index': 'test_index'
                }
            }
        ]

        self.assertEqual(bulk_ops, expected_bulk_ops)

    def test_process_changes(self):

        self.processor._es.bulk = mock.MagicMock(
            name='bulk',
            return_value={
                'errors': False
            }
        )

        super(
            SimpleESChangesProcessorTestCase,
            self
        ).test_process_changes()

        self.processor._es.bulk.assert_called_once_with(
            self.expected_bulk_ops,
            timeout=self.processor._bulk_timeout,
            request_timeout=self.processor._bulk_timeout
        )
