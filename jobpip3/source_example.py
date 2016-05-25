"""a test Source() that feeds records into a pipe"""

import random
from _source import Source
from record import Record


class ExampleRecord(Record):
    pass


class ExampleSource(Source):
    """simple example source generating ExampleRecords()"""

    def __init__(self, *args, **kwargs):
        """:param string: set value of string (default: "foo")
           :param int: set value of int (default: 1)
           :param float: set value of float (default: 1.5)
           :param list: set list (default: [1,2,3])
           :param dict: set dict (default: { '1' : 1, '2' : 2, '3' : 3 })"""
        super(ExampleSource, self).__init__(*args, **kwargs)

        # we will output TestRecord() objects
        self.OutRecord = ExampleRecord

        # handle arguments
        if 'string' in kwargs: self.string = unicode(kwargs['string'])
        else: self.string = "foo"
        if 'int' in kwargs: self.int = int(kwargs['int'])
        else: self.int = 1
        if 'float' in kwargs: self.float = float(kwargs['float'])
        else: self.float = 1.5
        if 'list' in kwargs: self.list = list(kwargs['list'])
        else: self.list = [1,2,3]
        if 'dict' in kwargs: self.dict = dict(kwargs['dict'])
        else: self.dict = { '1' : 1, '2' : 2, '3' : 3 }


    def well(self):

        for i in xrange(10):

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


