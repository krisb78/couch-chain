import multiprocessing

from . import base


class MPFeedReader(base.ChangesFeedReader):
    """Multiprocessing feed reader that spawns a new process for handling
    incoming _changes.

    """

    def __init__(self, **kwargs):

        self.changes_in, self.changes_out = multiprocessing.Pipe()

        super(
            MPFeedReader,
            self
        ).__init__(**kwargs)

        self._reader_process = multiprocessing.Process(
            target=self.read_changes
        )

        self._reader_process.start()

    def read_changes(self):
        """Reads changes from self.changes_out and processes them as normal.

        """

        while True:
            try:
                change_line = self.changes_out.recv()
            except EOFError:
                break

            if not change_line:
                self.process_heartbeat()
            else:
                self.process_change_line(change_line)

    def on_message(self, change_line):
        self.changes_in.send(change_line)

    def on_heartbeat(self):
        self.changes_in.send('')

    def wait_for_reader(self):
        self._reader_process.join()


class MPChangesConsumer(base.BaseChangesConsumer):
    """A changes consumer that runs 2 processes: one for reading
    the _changes stream and one for processing it.

    """

    feed_reader_class = MPFeedReader

    def consume(self):

        super(
            MPChangesConsumer,
            self
        ).consume()

        self._feed_reader.wait_for_reader()
