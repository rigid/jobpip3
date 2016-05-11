"""a test Source() that feeds records into a pipe"""

import random
from source import Source
from record import Record


class ExampleRecord(Record):
    pass


class ExampleSource(Source):

    def __init__(self, *args, **kwargs):
        super(ExampleSource, self).__init__(*args, **kwargs)

        # we will output TestRecord() objects
        self.OutRecord = ExampleRecord


    def well(self):

        for i in xrange(10):

            record = ExampleRecord({
                'test' : "foo",
                'int' : 1,
                'float' : 1.5,
                'list' : [1,2,3],
                'dict' : { '1' : 1, '2' : 2, '3' : 3 },
                'random' : random.random()
            })

            # output record
            yield record


