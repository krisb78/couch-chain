import copy
import logging

import boto
import elasticsearch
import pycouchdb

from concurrent import futures


logger = logging.getLogger(__name__)


class BaseChangesProcessor(object):

    def process_changes(self, changes_buffer):
        """Extracts processed docs from the changes buffer.

        :param changes_buffer: the list of changes fetched from the _changes
            stream.

        :returns: the list of processed docs.

        """

        processed_items = []

        for change_line in changes_buffer:
            item = self.process_change_line(change_line)

            if item is not None:
                processed_items.append(item)

        last_seq = changes_buffer[-1]['seq']

        return processed_items, last_seq

    def process_change_line(self, change_line):
        raise NotImplementedError


class BaseDocChangesProcessor(BaseChangesProcessor):
    """A base processor to use when documents are fetched with the
    changes. It returns documents fetched with changes.
    Override the `process_change_line` method to do any modifications
    on these documents.

    """

    def __init__(self, seq_property='_seq'):

        self._seq_property = seq_property

    def process_change_line(self, change_line):
        """Processes a single change line. Override this to modify how each
        document returned with a change line is processed.

        :returns: the document extracted from the line, with the relevant
            change sequence injected to it, if needed.

        """

        original_doc = change_line.get('doc')

        if original_doc is None:
            if change_line.get('deleted'):
                rev = change_line['changes'][0]['rev']
                doc = {
                    '_id': change_line['id'],
                    '_deleted': True,
                    '_rev': rev,
                }
            else:
                logger.info('Skipping change line: %s', change_line)
                return
        else:
            doc = copy.deepcopy(original_doc)

        seq_property = self._seq_property

        if seq_property is not None:
            seq = change_line.get('seq')

            if seq is not None:
                doc[seq_property] = seq

        return doc


class BaseESChangesProcessor(BaseDocChangesProcessor):

    def __init__(self, es_urls, es_index, **kwargs):
        """

        :param es_urls: Urls of ES nodes.
        :param es_index: the name of the index to store documents in.

        """

        super(
            BaseESChangesProcessor,
            self
        ).__init__(**kwargs)

        self._es = elasticsearch.Elasticsearch(es_urls)
        self._es_index = es_index


class BaseCouchdbChangesProcessor(BaseDocChangesProcessor):

    def __init__(self, target_couchdb_uri, target_couchdb_name, **kwargs):
        """

        :param target_couchdb_uri: the uri of the couchdb server to forward
            changes to.
        :param target_couchdb_name: the name of the target database.

        """

        super(
            BaseCouchdbChangesProcessor,
            self
        ).__init__(**kwargs)

        target_server = pycouchdb.Server(target_couchdb_uri)
        self._target_couchdb = target_server.database(target_couchdb_name)


class BaseS3ChangesProcessor(BaseDocChangesProcessor):
    """Backs documents up onto s3.

    """

    def __init__(
        self,
        bucket_name,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        max_workers=1,
        **kwargs
    ):
        """Initialise S3 connections and thread pool executor to run futures
        on.

        :param bucket_name: the name of the s3 bucket to upload documents to.
        :param aws_access_key_id: the AWS access key id.
        :param aws_secret_access_key: the AWS secret access key.
        :param max_workers: the maximum number of thread to run futures on.
            Don't set to None or it will hang (at least on macosx).
        """
        super(
            BaseS3ChangesProcessor,
            self
        ).__init__(**kwargs)

        conn = boto.connect_s3(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        bucket = conn.get_bucket(bucket_name)

        self._bucket = bucket

        # There's no "bulk put" is s3, so create an executor for uploading
        # documents in parallel to s3
        self._executor = futures.ThreadPoolExecutor(
            max_workers=max_workers
        )

    def cleanup(self):
        self._executor.shutdown()
