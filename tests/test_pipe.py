
import sys
import unittest
from jobpip3.sources import ExampleSource
from jobpip3.functions import ExampleFunction
from jobpip3 import Pipe



class TestPip3(unittest.TestCase):
    """jobpip3 tests"""


    def testsources(
        self,
        count=None,
        mode=None,
        parallel_workers=None,
    ):
        """create possible source elements"""

        # possible modes for source elements
        if count is None: count = [ 20, 100 ]
        else: count = [ count ]
        if mode is None: mode = [ 'internal', 'subprocess' ]
        else: mode = [ mode ]
        if parallel_workers is None: parallel_workers = [ 1, 5 ]
        else: parallel_workers = [ parallel_workers ]

        # generate variations of the ExampleSource element
        for c in count:
            for m in mode:
                for p in parallel_workers:
                    yield ExampleSource(
                        count=c,
                        mode=m,
                        parallel_workers=p,
                        string="test",
                        i=5,
                        f="Inf",
                        l=[4,5,6],
                        d={'x':1, 'y':0}
                    )

    def testfunctions(
        self,
        quick=None,
        mode=None,
        parallel_workers=None,
        worker_limit=None
    ):
        """create possible function elements"""

        # possible modes for function elements
        if quick is None: quick = [ False, True ]
        else: quick = [ quick ]
        if mode is None: mode = [ 'internal', 'subprocess' ]
        else: mode = [ mode ]
        if parallel_workers is None: parallel_workers = [ 1, 5 ]
        else: parallel_workers = [ parallel_workers ]
        if worker_limit is None: worker_limit = [ 0, 1, 10 ]
        else: worker_limit = [ worker_limit ]


        # generate variations of the ExampleFunction element
        for q in quick:
            for m in mode:
                for p in parallel_workers:
                    for w in worker_limit:
                        if m == 'internal' and w > 0:
                            continue

                        yield ExampleFunction(
                            mode=m,
                            quick=q,
                            parallel_workers=p,
                            worker_limit=w
                        )

    def assert_example_record(self, record, src, func):
        self.assertEqual(
            record['int'],
            5+1,
            msg="{} -> {} ({} != {})".format(
                src, func, record['int'], 5+1
            )
        )
        self.assertEqual(
            record['float'],
            float("Inf"),
            msg="{} -> {} ({} != {})".format(
                src, func, record['float'], float("Inf")
            )
        )
        self.assertEqual(
            record['list'],
            [4,5,6],
            msg="{} -> {} ({} != {})".format(
                src, func, record['list'], [4,5,6]
            )
        )
        self.assertEqual(
            record['dict'],
            {'x':1, 'y':0},
            msg="{} -> {} ({} != {})".format(
                src, func, record['dict'], {'x':1, 'y':0}
            )
        )


    def test_internal_quick(self):
        """test quick example pipe in mode='internal'"""
        for src in self.testsources(mode='internal', parallel_workers=1):
            for func in self.testfunctions(quick=True, mode='internal', worker_limit=0, parallel_workers=1):
                pipe = Pipe(src, [ func ])

                for record in pipe.run():
                    self.assert_example_record(record, src, func)


    def test_internal_slow(self):
        """test example pipe with randomly delaying Function() in mode='internal'"""
        for src in self.testsources(mode='internal', parallel_workers=1):
            for func in self.testfunctions(quick=False, mode='internal', worker_limit=0, parallel_workers=1):
                pipe = Pipe(src, [ func ])

                for record in pipe.run():
                    self.assert_example_record(record, src, func)


    def test_subprocess_quick(self):
        """test example pipe in mode='subprocess'"""
        for src in self.testsources(mode='subprocess', count=100):
            for func in self.testfunctions(
                mode='subprocess', quick=True, worker_limit=0
            ):
                pipe = Pipe(src, [ func ])

                for record in pipe.run():
                    self.assert_example_record(record, src, func)


    def test_subprocess_quick_worker_limit(self):
        """test example pipe in mode='subprocess' with worker_limt != 0"""
        for src in self.testsources(mode='subprocess', count=100):
            for func in self.testfunctions(
                mode='subprocess', quick=True, worker_limit=10
            ):
                pipe = Pipe(src, [ func ])

                for record in pipe.run():
                    self.assert_example_record(record, src, func)


    def test_subprocess_slow(self):
        """test example pipe with randomly delaying Function() in mode='subprocess'"""
        for src in self.testsources(mode='subprocess', count=100):
            for func in self.testfunctions(mode='subprocess', quick=False):
                pipe = Pipe(src, [ func ])

                for record in pipe.run():
                    self.assert_example_record(record, src, func)


    #~ def test_mixed(self):
        #~ """test example pipe with mixed modes"""
        #~ for src in self.testsources():
            #~ for func in self.testfunctions(quick=True):
                #~ pipe = Pipe(src, [ func ])

                #~ for record in pipe.run():
                    #~ self.assert_example_record(record, src, func)





