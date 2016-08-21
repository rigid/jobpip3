
from .._element import Element


class Source(Element):
    """Source() base class: a Source() generates records and passes them down
       the pipe"""

    def __init__(self, *args, **kwargs):
        # source doesn't receive records (it generates them)
        self.has_input = False
        # source outputs records
        self.has_output = True

        super(Source, self).__init__(*args, **kwargs)


    def worker(self, records):
        """actual worker method"""
        return self.well()


    def well(self):
        raise NotImplementedError(
            """class "{0}" needs a well() method""".format(
                self.__class__.__name__
            )
        )
