import logging
import os

import pycouchdb


logger = logging.getLogger(__name__)


class BaseSeqTracker(object):

    def put_seq(self, seq):
        """Saves the change seq.

        :param seq: couchdb change sequence id.

        """

        raise NotImplementedError

    def get_seq(self):
        """Loads the last know change sequence.

        """

        raise NotImplementedError

    def cleanup(self):
        """Close files, connections, etc. here.

        """

        raise NotImplementedError


class FilebasedSeqTracker(BaseSeqTracker):
    """Keeps a change sequence in a file.

    """

    def __init__(self, file_path):

        if not os.path.exists(file_path):
            open(file_path, 'wb').close()
        self._seq_file = open(file_path, 'r+b')

    def put_seq(self, seq):

        seq_file = self._seq_file

        seq_file.seek(0)

        seq_file.write(str(seq))
        seq_file.truncate()
        seq_file.flush()
        logger.info('Put seq: %s', seq)

    def get_seq(self):

        seq_file = self._seq_file

        seq_file.seek(0)
        seq = seq_file.read()

        logger.info('Got seq: %s', seq)

        return seq

    def cleanup(self):

        self._seq_file.close()


class CouchDBSeqTracker(BaseSeqTracker):

    def __init__(self, couchdb_uri, couchdb_name, seq_doc_id):
        """Reads the current version of the sequence documents from
        the database and stores it on the instance.

        """

        server = pycouchdb.Server(couchdb_uri)
        self._couchdb = server.database(couchdb_name)

        try:
            seq_doc = self._couchdb.get(seq_doc_id)
        except pycouchdb.exceptions.NotFound:
            seq_doc = {
                '_id': seq_doc_id,
            }

        self._seq_doc = seq_doc

        super(
            CouchDBSeqTracker,
            self
        ).__init__()

    def put_seq(self, seq):
        """Writes the new seq to the database. Will break when there is an
        update conflict.

        """

        self._seq_doc['seq'] = seq

        self._seq_doc = self._couchdb.save(self._seq_doc)
        logger.info('Put seq: %s', seq)

    def get_seq(self):
        """Returns the current sequence known by the tracker.

        """

        seq = self._seq_doc.get('seq', '')

        logger.info('Got seq: %s' % seq)

        return seq

    def cleanup(self):
        pass
