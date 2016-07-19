"""a source that reads records from one or more files"""

import os
import importlib
import types
import itertools

from ..util import log
from .. import records
from ..records import Record
from . import Source



class Readfile(Source):
    """read records from file(s)"""

    def __init__(self, files, record_class="Record", **kwargs):
        """:param files: filename or list of filenames
           :param record_class: class of records that will
                be read (default: Record)"""

        if isinstance(record_class, (str, unicode)):
            record_class = getattr(records, record_class)

        super(Readfile, self).__init__(
            files=files,
            record_class=record_class.__name__,
            **kwargs
        )


    def init(self, files, record_class, **kwargs):
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
