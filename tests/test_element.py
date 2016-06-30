
import unittest
from jobpip3 import Record
from jobpip3._element import Element


RECORD_AMOUNT = 3


class Test1(Element):

    def __init__(self, *args, **kwargs):
        super(Test1, self).__init__(*args, **kwargs)
        self.InRecord = None

    def worker(self, records):
        for i in xrange(RECORD_AMOUNT):
            yield Record({ 'i' : i, 'foo' : "bar" })



class TestElement(unittest.TestCase):
    """Element() tests"""

    def test_output(self):

        e = Test1(mode='internal')

        i = 0
        for r in e.flow():
            self.assertEqual(r['i'], i)
            self.assertEqual(r['foo'], "bar")
            i += 1

        self.assertEqual(i, RECORD_AMOUNT)
