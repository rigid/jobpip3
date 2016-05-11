"""a test Function() that takes records from a pipe, applies a function to
   them and passes them further down the pipe"""


from function import Function




class ExampleFunction(Function):


    def process(self, records):

        for record in records:

            # add string
            record['added'] = "bar"

            # change string
            record['test'] = "changed"

            if 'int' in record:
                record['int'] += 1
            else:
                record['int'] = 1

            # output record
            yield record
