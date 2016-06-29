
from itertools import chain
from .util import log
from _source import Source



class Pipe(object):
    """bundle source, functions and a sink"""

    def __init__(self, source, functions=[], sink=None):
        # pipe source
        self._source = source
        # create list of functions if necessary
        if not isinstance(functions, list): functions = [ functions ]
        self._functions = functions
        # pipe sink
        self._sink = sink


    def __str__(self):
        return "<Pipe(source={} -> funcs=[{}] -> sink={})>".format(
            self._source.__class__.__name__ + "()",
            " -> ".join([ f.__class__.__name__ + "()" for f in self._functions ]),
            "None" if self._sink.__class__.__name__ is None \
                else self._sink.__class__.__name__ + "()"
        )


    def run(self):
        """run pipe"""

        log.debug("running pipe: {}".format(self))

        # iterate all sources
        records = self._source.flow()

        # iterate all processors
        for function in self._functions:
            r = function.flow(records)
            records = r

        # got no sink?
        if self._sink is None:
            # return records
            return records

        # pass records to sink
        for r in self._sink.flow(records): pass

        # return nothing
        return
