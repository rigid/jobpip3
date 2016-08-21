
from .._element import Element


class Sink(Element):
    """Sink() base class: a Sink() takes records from the pipe and
       drains them somewhere."""

    def __init__(self, *args, **kwargs):
        # a sink receives records
        self.has_input = True
        # a sink doesn't produce output records
        self.has_output = False

        super(Sink, self).__init__(*args, **kwargs)

    @staticmethod
    def _del_internal(records):
        """remove internal attributes before passing record to sink"""
        for r in records:
            for k in r.keys():
                if k.startswith("__") and k.endswith("__"): del r[k]
            yield r


    def worker(self, records):
        """actual worker method"""
        self.drain(self._del_internal(records))
        return []


    def drain(self, record):
        raise NotImplementedError("every Sink() needs a drain() method")
