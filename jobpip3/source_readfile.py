"""a source that reads records from one or more files"""

import types
import itertools

from _source import Source
from record import Record



class Readfile(Source):
    """read records from file(s)"""

    def __init__(self, files, record_class=Record, **kwargs):
        """:param files: fd or list of fd objects
           :param record_class: class of records that will
                be read (default: Record)"""
        super(Readfile, self).__init__(**kwargs)

        # handle arguments
        self.files = files
        if not isinstance(self.files, (list, types.GeneratorType)): self.files = [ self.files ]
        self.OutRecord = record_class


    def well(self):

        for fd in self.files:
            for record in self.OutRecord.read(fd):
                # output record
                yield record

