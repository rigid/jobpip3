
from panoracle.util import log
from _element import Element


class Function(Element):
    """Function() base class: a Function() takes records from the pipe,
       applies a function to them and passes them further down the pipe"""


    def __init__(self, *args, **kwargs):
        super(Function, self).__init__(*args, **kwargs)

        # total amount of records seen by this processor (passed + dismissed)
        self.total = 0
        # amount of records that passed through this processor
        self.passed = 0
        # amount of records that were not passed on
        self.dismissed = 0
        # amount of records that were touched by this processor
        self.modified = 0


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


    def status(self):
        """print current state of this processor"""

        # calculate missing values
        if self.total == 0 and (self.passed != 0 or self.dismissed != 0):
            self.total = self.passed + self.dismissed
        elif self.passed == 0 and (self.total != 0 or self.dismissed != 0):
            self.passed = self.total - self.dismissed
        elif self.dismissed == 0 and (self.total != 0 or self.passed != 0):
            self.dismissed = self.total - self.passed

        log.info("{0}: passed: {2}, dismissed: {3}, "
                 "modified: {4}, total: {1}".format(
                self.__class__.__name__,
                self.total, self.passed, self.dismissed,
                self.modified
            ))
