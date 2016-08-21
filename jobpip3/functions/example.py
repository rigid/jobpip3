"""a test Function() that takes records from a pipe, applies a function to
   them and passes them further down the pipe"""


import time
import random

from . import Function


class ExampleFunction(Function):

    def init(self, quick=False, **kwargs):

        # don't sleep if quick is True
        self.quick = quick


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

