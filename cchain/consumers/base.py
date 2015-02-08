import datetime
import logging
import pycouchdb

from cchain.processors import exceptions as processors_exceptions


logger = logging.getLogger(__name__)


class ChangesFeedReader(pycouchdb.feedreader.BaseFeedReader):

    def __init__(
        self,
        limit=1000,
        flush_interval=10,
        processor=None,
        seqtracker=None
    ):

        self._limit = limit
        self._flush_interval = flush_interval
        self._processor = processor
        self._seqtracker = seqtracker
        self._last_flush_time = datetime.datetime.now()

        self._buffer = []

    def process_change_line(self, change_line):
        change = change_line.get('changes')

        if change is None:
            logger.debug('Skipping change line: %s', change_line)
        else:
            self._buffer.append(change_line)
            logger.debug('Change added to buffer: %s', change_line)

        now = datetime.datetime.now()

        waiting_time = now - self._last_flush_time

        waits_too_long = waiting_time > self._flush_interval

        if (len(self._buffer) >= self._limit) or waits_too_long:
            self.flush_buffer()

    def process_heartbeat(self):
        logger.debug('Heartbeat received.')
        self.flush_buffer()

    def on_message(self, change_line):
        logger.debug('Change received: %s', change_line)
        self.process_change_line(change_line)

    def on_heartbeat(self):
        self.process_heartbeat()

    def flush_buffer(self):
        if not self._buffer:
            return

        try:
            _, last_seq = self._processor.process_changes(
                self._buffer
            )
        except processors_exceptions.ProcessingError:
            raise pycouchdb.exceptions.FeedReaderExited

        self._last_flush_time = datetime.datetime.now()
        # Only save the last sequence if all the changes have been
        # successfully processed.
        if last_seq is not None:
            self._seqtracker.put_seq(last_seq)

        self._buffer = []

    def cleanup(self):
        self.flush_buffer()


class BaseChangesConsumer(object):

    feed_reader_class = ChangesFeedReader

    def __init__(
        self,
        couchdb_uri,
        couchdb_name,
        feed_kwargs=None,
        limit=1000,
        flush_interval=10,
        processor=None,
        seqtracker=None
    ):
        """Initialises the consumer.

        :param couchdb_uri: the uri of your couchdb server.
        :param couchdb_name: the name of the database you want changes from.
        :param feed_kwargs: the arguments to be passed to the feed url.
        :param limit: maximum number of changes to be processed in a batch.
        :param flush_interval: the maximum time, in seconds, to wait before
            processing a batch of changes.
        :param processor: a subclass of
            `cchain.processors.base.BaseChangesProcessor`.
        :param seqtracker: a subclass of
            `cchain.seqtrackers.base.BaseSeqTracker`.

        """

        server = pycouchdb.Server(couchdb_uri)
        self._couchdb = server.database(couchdb_name)
        self._limit = limit
        self._flush_interval = datetime.timedelta(seconds=flush_interval)
        self._processor = processor
        self._seqtracker = seqtracker

        default_feed_kwargs = {
            'include_docs': 'true',
        }

        feed_kwargs = feed_kwargs or {}

        default_feed_kwargs.update(feed_kwargs)

        self._feed_kwargs = default_feed_kwargs

        self._buffer = []

        self._feed_reader = self.feed_reader_class(
            limit=self._limit,
            flush_interval=self._flush_interval,
            processor=self._processor,
            seqtracker=self._seqtracker

        )

    def consume(self):
        """Processes the changes stream.

        """

        last_seq = self._seqtracker.get_seq()

        feed_kwargs = self._feed_kwargs

        if last_seq:
            feed_kwargs.update({
                'since': last_seq,
            })

        self._couchdb.changes_feed(
            self._feed_reader,
            **feed_kwargs
        )

        self._feed_reader.cleanup()

        self._seqtracker.cleanup()
