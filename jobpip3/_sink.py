
from _element import Element


class Sink(Element):
    """Sink() base class: a Sink() takes records from the pipe and
       drains them somewhere."""

    def __init__(self, *args, **kwargs):
        super(Sink, self).__init__(*args, **kwargs)
        # a sink doesn't produce output records
        self.OutRecord = None


    def worker(self, records):
        """actual worker method"""
        return self.drain(records)


    def drain(self, record):
        raise NotImplementedError("every Sink() needs a drain() method")
