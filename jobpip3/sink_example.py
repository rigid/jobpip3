"""a test Sink() that takes records from a pipe and dumps them somewhere"""


from _sink import Sink




class ExampleSink(Sink):

    def __init__(self):
        self._count = 0


    def drain(self, record):
        record['added'] = "bar"
        self._count += 1
        print record.dump()
