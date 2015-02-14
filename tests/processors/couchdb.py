import unittest

import cchain
import mock

from . import mixins
from . import samples


class SimpleCouchdbChangesProcessorTestCase(
    unittest.TestCase,
    mixins.ProcessChangesTestMixin
):

    def setUp(self):
        with mock.patch('pycouchdb.Server'):
            self.processor = (
                cchain.processors.couchdb.SimpleCouchdbChangesProcessor(
                    'http://localhost:5984',
                    'test_db'
                )
            )
        self.samples = samples.CHANGES_DOCS

        self.expected_results = [
            self.processor.process_change_line(sample)
            for sample in self.samples
        ]

    def test_process_changes(self):

        doc_ids = [
            sample['id'] for sample in self.samples
        ]

        self.processor._target_couchdb.all = mock.MagicMock(name='all')
        self.processor._target_couchdb.save_bulk = mock.MagicMock(
            name='save_bulk'
        )

        super(
            SimpleCouchdbChangesProcessorTestCase,
            self
        ).test_process_changes()

        self.processor._target_couchdb.all.assert_called_once_with(
            keys=doc_ids
        )

        expected_docs = [
            doc for doc, rev, seq in self.expected_results
        ]

        self.processor._target_couchdb.save_bulk.assert_called_once_with(
            expected_docs
        )
