"""a test Function() that takes records from a pipe, applies a function to
   them and passes them further down the pipe"""


from function import Function
from source_example import ExampleRecord



class ExampleFunction(Function):

    def __init__(self, *args, **kwargs):
        super(ExampleFunction, self).__init__(*args, **kwargs)

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

            # output record
            yield record
