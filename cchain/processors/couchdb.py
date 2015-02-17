import logging

from cchain.processors import base
from cchain.processors import exceptions


logger = logging.getLogger(__name__)


class SimpleCouchdbChangesProcessor(base.BaseCouchdbChangesProcessor):
    """Allows to replicate documents into another database, trasnforming
    them first if necessary.

    """

    def __init__(
        self,
        target_couchdb_uri,
        target_couchdb_name,
        **kwargs
    ):
        """

        :param target_couchdb_uri: the uri of the couchdb server to forward
            changes to.
        :param target_couchdb_name: the name of the target database.
        :param seq_property: the name of the property that will be added
            to the document to store the relevant change sequence. If `None`,
            it won't be added at all.

        """

        super(
            SimpleCouchdbChangesProcessor,
            self
        ).__init__(target_couchdb_uri, target_couchdb_name, **kwargs)

    def process_changes(self, changes_buffer):
        """Saves processed documents in the target couchdb.

        """

        processed_items, last_seq = super(
            SimpleCouchdbChangesProcessor,
            self
        ).process_changes(changes_buffer)

        if not processed_items:
            return processed_items, last_seq

        doc_ids = []
        processed_docs = []

        for (doc, rev, seq, ) in processed_items:
            doc_ids.append(doc['_id'])
            processed_docs.append(doc)

        existing_results = self._target_couchdb.all(
            keys=doc_ids
        )

        for existing_result, processed_doc in zip(
            existing_results, processed_docs
        ):

            value = existing_result.get('value')
            if value is not None:
                processed_doc['_rev'] = value['rev']

        error = False

        try:
            bulk_results = self._target_couchdb.save_bulk(processed_docs)
        except:
            logger.exception('Failed to insert documents')
            error = True
        else:
            for bulk_result in bulk_results:
                if bulk_result.get('error'):
                    logger.error('Errors executing bulk!')
                    error = True
                    break

        if error:
            raise exceptions.ProcessingError

        return processed_items, last_seq
