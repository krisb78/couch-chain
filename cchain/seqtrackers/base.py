import logging
import os


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


class FilebasedSeqTracker(object):
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
