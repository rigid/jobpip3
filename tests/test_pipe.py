
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
        worker_limit=None
    ):
        """create possible source elements"""

        # possible modes for source elements
        if count is None: count = [ 50, 1000 ]
        else: count = [ count ]
        if mode is None: mode = [ 'internal', 'subprocess' ]
        else: mode = [ mode ]
        if parallel_workers is None: parallel_workers = [ 1, 5 ]
        else: parallel_workers = [ parallel_workers ]
        if worker_limit is None: worker_limit = [ 0, 1, 10 ]
        else: worker_limit = [ worker_limit ]

        # generate variations of the ExampleSource element
        for c in count:
            for m in mode:
                for p in parallel_workers:
                    for w in worker_limit:
                        yield ExampleSource(
                            count=c,
                            mode=m,
                            parallel_workers=p,
                            worker_limit=w,
                            string="test",
                            int=5,
                            float="Inf",
                            list=[4,5,6],
                            dict={'x':1, 'y':0}
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
                        yield ExampleFunction(
                            mode=m,
                            quick=q,
                            parallel_workers=p,
                            worker_limit=w
                        )



    def test_internal(self):
        """test example pipe with mode='internal'"""
        for src in self.testsources(mode='internal', worker_limit=0, parallel_workers=1):
            for func in self.testfunctions(mode='internal', worker_limit=0, parallel_workers=1):
                pipe = Pipe(src, [ func ])

                for record in pipe.run():
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



    def test_subprocess(self):
        """test example pipe with mode='subprocess'"""
        for src in self.testsources(mode='subprocess', count=1):
            for func in self.testfunctions(mode='subprocess', quick=True):
                pipe = Pipe(src, [ func ])

                for record in pipe.run():
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


    def test_mixed(self):
        """test example pipe with mixed modes"""
        for src in self.testsources(parallel_workers=1, worker_limit=0):
            for func in self.testfunctions(quick=True, parallel_workers=1, worker_limit=0):
                pipe = Pipe(src, [ func ])

                for record in pipe.run():
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


    #~ def test_variation(self):
        #~ """test various combinations of pipe elements"""
        #~ for src in self.sources():
            #~ for func in self.functions():
                #~ r = src.flow()
                #~ r = func.flow(r)
                #~ length = 0
                #~ for record in r:
                    #~ self.assertEqual(
                        #~ record['int'],
                        #~ 5+1,
                        #~ msg="{} -> {} ({} != {})".format(
                            #~ src, func, record['int'], 5+1
                        #~ )
                    #~ )
                    #~ self.assertEqual(
                        #~ record['float'],
                        #~ float("Inf"),
                        #~ msg="{} -> {} ({} != {})".format(
                            #~ src, func, record['float'], float("Inf")
                        #~ )
                    #~ )
                    #~ self.assertEqual(
                        #~ record['list'],
                        #~ [4,5,6],
                        #~ msg="{} -> {} ({} != {})".format(
                            #~ src, func, record['list'], [4,5,6]
                        #~ )
                    #~ )
                    #~ self.assertEqual(
                        #~ record['dict'],
                        #~ {'x':1, 'y':0},
                        #~ msg="{} -> {} ({} != {})".format(
                            #~ src, func, record['dict'], {'x':1, 'y':0}
                        #~ )
                    #~ )
                    #~ length += 1

                #~ self.assertEqual(
                    #~ length,
                    #~ 10*src.parallel_workers,
                    #~ msg="{} -> {} (got {} instead of {} records)".format(
                        #~ src, func, length, 10*src.parallel_workers
                    #~ )
                #~ )


                #~ #sys.stderr.write(".")
                #~ #sys.stderr.flush()


