"""a test Function() that takes records from a pipe, applies a function to
   them and passes them further down the pipe"""


import time
import random
from ._function import Function
from ..source.example import ExampleRecord



class ExampleFunction(Function):

    def __init__(self, *args, **kwargs):
        super(ExampleFunction, self).__init__(*args, **kwargs)

        # don't sleep if quick is True
        if 'quick' in kwargs: self.quick = kwargs['quick']
        else: self.quick = False

        self.InRecord = ExampleRecord
        self.OutRecord = ExampleRecord


    def process(self, records):

        for record in records:

            # add string
            record['added'] = "bar"

            if 'int' in record:
                record['int'] += 1
            else:
                record['int'] = 1

            # don't simulate workload?
            if not self.quick:
                # sleep random amount of time to simulate workload
                time.sleep(random.random()/10)

            # output record
            yield record

