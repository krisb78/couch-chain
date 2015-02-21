import multiprocessing

from . import base


class MPFeedReader(base.ChangesFeedReader):
    """Multiprocessing feed reader that spawns a new process for handling
    incoming _changes.

    """

    def __init__(self, **kwargs):

        self._changes_in, self._changes_out = multiprocessing.Pipe()

        super(
            MPFeedReader,
            self
        ).__init__(**kwargs)

        self._reader_process = multiprocessing.Process(
            target=self.read_changes
        )

        self._reader_process.start()

        # The main process will only write to the pipe, so close the
        # output end.
        self._changes_out.close()

    def read_changes(self):
        """Reads changes from self._changes_out and processes them as normal.

        """

        # Close the input end, because we will read from the pipe here.
        self._changes_in.close()

        while True:
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
