import datetime
import queue

import futures
import logging
import multiprocessing
import pycouchdb

from . import base


logger = logging.getLogger(__name__)


class MPFeedReader(base.ChangesFeedReader):
    """Multiprocessing feed reader that spawns a new process for handling
    incoming _changes.

    """

    task_queue_length = 10

    def __init__(self, **kwargs):

        self._changes_in, self._changes_out = multiprocessing.Pipe()

        super(
            MPFeedReader,
            self
        ).__init__(**kwargs)

        self._reader_process = multiprocessing.Process(
            target=self.start_reading_changes
        )

        self._reader_process.start()

        # The main process will only write to the pipe, so close the
        # output end.
        self._changes_out.close()

    def _track_seq(self):
        """Reads persist tasks from the queue and keeps track of the
        last change seqence.

        """

        while True:

            logger.debug('Waiting for data to persist...')
            (processed_changes, last_seq) = self._persist_queue.get()

            if processed_changes is None:
                logger.info('Terminating sequence tracker.')
                self._persist_queue.task_done()
                break

            logger.debug('Got processed_changes with last seq: %s', last_seq)
            # If this succeeds, go on and save the sequence to the file,
            # otherwise break spectacularly.
            if processed_changes:
                self._processor.persist_changes(processed_changes)

            self._persist_queue.task_done()

            if last_seq is not None:
                self._seqtracker.put_seq(last_seq)

    def flush_buffer(self):
        if not self._buffer:
            return

        try:
            processed_changes, last_seq = self._processor.process_changes(
                self._buffer
            )

            self._buffer = []
            self._last_flush_time = datetime.datetime.now()

            logger.info(
                'Putting processed data in the queue, last_seq: %s',
                last_seq
            )

            self._persist_queue.put((processed_changes, last_seq))

        except:
            logger.exception('Error while processing changes!')
            raise pycouchdb.exceptions.FeedReaderExited

    def start_reading_changes(self):
         # Close the input end, because we will read from the pipe here.
        self._changes_in.close()

        # The queue to store `persist_changes` tasks on.
        self._persist_queue = queue.Queue(maxsize=self.task_queue_length)

        self._persist_executor = futures.ThreadPoolExecutor(
            max_workers=1
        )

        # Start the future for tracking the sequence.
        self._seq_tracking_future = self._persist_executor.submit(
            self._track_seq
        )

        # When tracker dies, purge the queue to prevent deadlocks.
        # TODO(kris): Sort it out properly.
        def purge_queue(_):
            while True:
                try:
                    self._persist_queue.get_nowait()
                    self._persist_queue.task_done()
                except queue.Empty:
                    break

        # Purge the queue if nobody is reading from it anymore.
        self._seq_tracking_future.add_done_callback(
            purge_queue
        )

        # Now, read changes until you die.
        try:
            self.read_changes()
        except:
            logger.exception('Error reading changes! Terminating.')

        # Write a termination sequence to the queue, so that the sequence
        # tracker can exit.
        logger.debug('Writing termination sequence to persist queue...')
        self._persist_queue.put((None, None))

    def read_changes(self):
        """Reads changes from self._changes_out and processes them as normal.

        """

        while self._seq_tracking_future.running():
            try:
                change_line = self._changes_out.recv()
            except EOFError:
                break

            if not change_line:
                self.process_heartbeat()
            else:
                self.process_change_line(change_line)

    def on_message(self, change_line):
        self._changes_in.send(change_line)

    def on_heartbeat(self):
        self._changes_in.send('')

    def cleanup(self):
        super(
            MPFeedReader,
            self
        ).cleanup()

        self._changes_in.close()

        self._reader_process.join()


class MPChangesConsumer(base.BaseChangesConsumer):
    """A changes consumer that runs 2 processes: one for reading
    the _changes stream and one for processing it.

    """

    feed_reader_class = MPFeedReader
