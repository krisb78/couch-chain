

class ProcessChangesTestMixin(object):
    """A mixin defining a base method for testing `process_changes`.

    """

    def test_process_changes(self):

        (processed_changes, seq, ) = self.processor.process_changes(
            self.samples
        )

        self.assertEqual(seq, self.samples[-1]['seq'])

        for sample, processed_change in zip(
            self.samples,
            processed_changes
        ):
            expected_change = self.processor.process_change_line(sample)
            self.assertEqual(processed_change, expected_change)
