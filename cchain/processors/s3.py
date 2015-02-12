import json
import logging

from cchain.processors import base


logger = logging.getLogger(__name__)


class SimpleS3ChangesProcessor(base.BaseS3ChangesProcessor):
    """Stores everything in s3.

    """

    def get_s3_key_name(self, doc):
        """Returns the key to store the document in s3 under.

        :param doc: the document to store in s3.

        """

        key = '%s/%s' % (
            doc['_id'],
            doc['_rev'],
        )

        return key

    def _store_doc_in_bucket(self, doc):

        key_name = self.get_s3_key_name(doc)

        key = self._bucket.new_key(key_name=key_name)

        doc_body = json.dumps(doc)

        key.set_contents_from_string(doc_body)

        return key_name

    def process_changes(self, changes_buffer):

        processed_items, last_seq = super(
            SimpleS3ChangesProcessor,
            self
        ).process_changes(changes_buffer)

        futures = self._executor.map(
            self._store_doc_in_bucket,
            processed_items
        )

        for future in futures:
            key_name = future.result()
            logger.debug('Key created in s3: %s', key_name)

        return processed_items, last_seq
