

import unittest
from jobpip3.record import Record




class TestRecord(unittest.TestCase):
    """Record() tests"""


    def test_dict(self):
        """a Record() mimics a dict. Test that here"""
        # create a record
        a = Record()

        # __setattr__
        a['foo'] = 2.5
        self.assertEqual(a['foo'], 2.5)

        # __getattr__
        self.assertEqual(a['foo'], 2.5)

        # keys
        self.assertTrue('foo' in a)


    def test_parse(self):
        """a Record() can be created by passing a dict. Test that here."""

        # create a record
        a = Record({ 'foo' : 5.0, 'bar': 'string' })

        self.assertEqual(a['foo'], 5.0)
        self.assertEqual(a['bar'], 'string')


    def test_serialization(self):
        """a Record() can be serialized to a string. Test that here."""

        # create record
        a = Record({ 'foo' : 5.0 })
        # serialize to string
        a_string = a.dump()

        # create record from that string (shortcut for Record().parse(string))
        b = Record(a_string)
        # serialize again
        b_string = b.dump()

        # compare strings
        self.assertEqual(a_string, b_string)
