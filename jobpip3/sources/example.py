"""a test Source() that feeds records into a pipe"""

import random
from . import Source
from ..records import Record


class ExampleRecord(Record):
    pass


class ExampleSource(Source):
    """simple example source generating ExampleRecords()"""

    def init(self, count=10, string="foo", i=1, f=1.5, l=[1,2,3],
             d={ '1' : 1, '2' : 2, '3' : 3 }, **kwargs):
        """:param count: amount of records this source will generate
           :param string: set value of string (default: "foo")
           :param i: set value of int (default: 1)
           :param f: set value of float (default: 1.5)
           :param l: set list (default: [1,2,3])
           :param d: set dict (default: { '1' : 1, '2' : 2, '3' : 3 })"""

        # we will output TestRecord() objects
        self.OutRecord = ExampleRecord

        # handle arguments
        self.count = int(count)
        self.string = unicode(string)
        self.int = int(i)
        self.float = float(f)
        self.list = list(l)
        self.dict = dict(d)


    def well(self):

        for i in xrange(self.count):

            record = ExampleRecord({
                'string' : self.string,
                'int' : self.int,
                'float' : self.float,
                'list' : self.list,
                'dict' : self.dict,
                'random' : random.random()
            })

            # output record
            yield record
            self.passed += 1

