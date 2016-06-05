
from itertools import chain
from _source import Source



class Pipe(object):
    """bundle source, functions and a sink"""

    def __init__(self, source, processors=[], sink=None):
        self._source = source
        if not isinstance(processors, list): processors = [ processors ]
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

        # got no sink?
        if self._sink is None:
            # return records
            return records

        # pass records to sink
        for r in self._sink.flow(records): pass

        # return nothing
        return
