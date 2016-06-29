"""a source that reads records from one or more files"""

import os
import importlib
import types
import itertools

from ..util import log
from ..record import Record
from ._source import Source



class Readfile(Source):
    """read records from file(s)"""

    def __init__(self, files, record_class="Record", **kwargs):
        """:param files: filename or list of filenames
           :param record_class: class of records that will
                be read (default: Record)"""

        if isinstance(record_class, (str, unicode)):
            package = os.path.split(os.path.dirname(__file__))[-1]
            record_module = importlib.import_module(package + ".record")
            record_class = getattr(record_module, record_class)

        super(Readfile, self).__init__(
            files=files,
            record_class=record_class.__name__,
            **kwargs
        )

        # handle arguments
        self.filenames = files
        if not isinstance(self.filenames, (list, types.GeneratorType)):
            self.filenames = [ self.filenames ]

        self.OutRecord = record_class


    def well(self):
        log.debug("reading files: {}".format(self.filenames))

        for file in self.filenames:
            with open(file, 'rb') as fd:
                for record in self.OutRecord.read(fd):
                    # output record
                    yield record
                    self.passed += 1
