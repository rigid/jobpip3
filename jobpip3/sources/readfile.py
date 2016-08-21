"""a source that reads records from one or more files"""

import os
import importlib
import types
import itertools

from ..util import log
from ..records import Record
from . import Source



class Readfile(Source):
    """read records from file(s)"""

    def __init__(self, files, **kwargs):
        """:param files: filename or list of filenames"""

        # @todo this will not serialize when files are fd's but we can't
        # check, because it can be a generator that can only be consumed once

        if not isinstance(files, (list, tuple, types.GeneratorType)):
            files = [ files ]

        super(Readfile, self).__init__(
            files=files,
            **kwargs
        )


    def init(self, files, **kwargs):
        """:param files: filename or list of filenames"""

        # open files if necessary
        self.files = []
        for f in files:
            if not isinstance(f, file):
                log.debug("reading from file: {}".format(f))
                fd = open(f)
            else:
                fd = f
            self.files += [ fd ]


    def well(self):
        for file in self.files:
            for record in Record.read(file):
                # output record
                yield record
                self.passed += 1
