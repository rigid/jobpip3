
from .._element import Element
from ..record import Record


class Source(Element):
    """Source() base class: a Source() generates records and passes them down
       the pipe"""

    def __init__(self, *args, **kwargs):
        super(Source, self).__init__(*args, **kwargs)
        # a source doesn't have input
        self.InRecord = None


    def worker(self, records):
        """actual worker method"""
        return self.well()


    def well(self):
        raise NotImplementedError(
            """class "{0}" needs a well() method""".format(
                self.__class__.__name__
            )
        )
