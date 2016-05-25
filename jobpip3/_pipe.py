

class Pipe(object):
    """bundle source, functions and a sink"""

    def __init__(self, source, processors=[], sink=None):
        self._source = source
        self._processors = processors
        self._sink = sink


    def run(self):
        """run pipe"""

        # iterate all sources
        records = self._source.flow()

        # iterate all processors
        for processor in self._processors:
            r = processor.flow(records)
            records = r

        # sink
        if self._sink is None:
            return records

        self._sink.flow(records)

        return
