import json
import logging

from cchain.processors import base


logger = logging.getLogger(__name__)


class SimpleS3ChangesProcessor(base.BaseS3ChangesProcessor):
    """Stores documents in an s3 bucket.

    """

    def get_s3_key_name(self, doc):
        """Returns the key to store the document in s3 under.

        :param doc: the document to store in s3.

        :returns: the key that the document will be stored under in s3.

        You may want to override this if your document ids contain characters
        may cause problems with retrieving data from the bucket (such as '/').

        """

        key = '%s/%s' % (
            doc['_id'],
            doc['_rev'],
        )

        return key

    def _store_doc(self, doc_info):
        """Stores the document in the s3 bucket.

        :param doc_info: a tuple comprising document to store, the revision
            "number" and the seq of the correnspoding change.

        :returns: the key that the document was stored under.

        """

        (doc, rev, seq, ) = doc_info

        key_name = self.get_s3_key_name(doc)

        key = self._bucket.new_key(key_name=key_name)

        doc_body = json.dumps(doc)

        key.set_contents_from_string(doc_body)
        key.set_metadata('seq', seq)

        return key_name

    def process_changes(self, changes_buffer):

        processed_changes, last_seq = super(
            SimpleS3ChangesProcessor,
            self
        ).process_changes(changes_buffer)

        logger.debug(
            'Starting %d tasks to store documents...',
            len(processed_changes)
        )

        key_names = self._executor.map(
            self._store_doc,
            processed_changes
        )

        logger.debug('Done.')

        for key_name in key_names:
            logger.debug('Key created in s3: %s', key_name)

        return processed_changes, last_seq
