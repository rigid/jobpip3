
from .._element import Element


class Function(Element):
    """Function() base class: a Function() takes records from the pipe,
       applies a function to them and passes them further down the pipe"""

    def __init__(self, *args, **kwargs):
        # a function gets records input
        self.has_input = True
        # a function outputs records
        self.has_output = True

        super(Function, self).__init__(*args, **kwargs)


    def worker(self, records):
        """actual worker method"""
        return self.process(records)


    def process(self, records):
        """take records, process them, output them (or not)"""

        raise NotImplementedError(
            """class "{0}" needs a process() method""".format(
                self.__class__.__name__
            )
        )
