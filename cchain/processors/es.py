import logging

from cchain.processors import base
from cchain.processors import exceptions


logger = logging.getLogger(__name__)


class SimpleESChangesProcessor(base.BaseESChangesProcessor):
    """Stores documents in elasticsearch.

    """

    def __init__(self, es_urls, es_index, es_type, **kwargs):
        """

        :param es_urls: Urls of ES nodes.
        :param es_index: the name of the index to store documents in.
        :param es_type: what the `_type` property will be set to in es docs.
        :param seq_property: the name of the property that will be added
            to the document to store the relevant change sequence. If `None`,
            it won't be added at all.

        """

        super(
            SimpleESChangesProcessor,
            self
        ).__init__(es_urls, es_index, **kwargs)

        self._es_type = es_type

    def process_changes(self, changes_buffer):
        """Sticks the processed documents into elasticsearch.

        """

        processed_docs, last_seq = super(
            SimpleESChangesProcessor,
            self
        ).process_changes(changes_buffer)

        if not processed_docs:
            return processed_docs, last_seq

        bulk_data = []

        for doc in processed_docs:
            op_data = {
                '_id': doc['_id'],
                '_type': self._es_type,
                '_index': self._es_index,
            }
            if doc.get('_deleted'):
                op_dict = {
                    'delete': op_data,
                }
                bulk_data.append(op_dict)
            else:
                op_dict = {
                    'index': op_data,
                }
                bulk_data.append(op_dict)
                bulk_data.append(doc)

        error = False

        try:
            return_value = self._es.bulk(
                bulk_data,
                refresh=True
            )
        except:
            logger.exception('Failed to index documents!')
            error = True
        else:
            if return_value.get('errors'):
                logger.error('Errors executing bulk!')
                error = True

        if error:
            raise exceptions.ProcessingError

        return processed_docs, last_seq
