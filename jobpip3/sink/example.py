"""a test Sink() that takes records from a pipe and dumps them somewhere"""


from ..util import log
from ._sink import Sink




class ExampleSink(Sink):

    def __init__(self, *args, **kwargs):
        super(ExampleSink, self).__init__(*args, **kwargs)

        self._count = 0


    def drain(self, records):
        for record in records:
            record['added'] = "bar"
            log.info(record)
            self._count += 1
