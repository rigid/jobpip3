
import sys
import unittest
from jobpip3.sources import ExampleSource
from jobpip3.functions import ExampleFunction



class TestPip3(unittest.TestCase):
    """jobpip3 tests"""


    def sources(self):
        """create possible source elements"""
        # variations of mode='subprocess')
        for workers in [1, 5]:
            yield ExampleSource(
                mode='subprocess',
                parallel_workers=workers,
                string="workers-test-{}".format(workers),
                int=5,
                float="Inf",
                list=[4,5,6],
                dict={'x':1, 'y':0}
            )

        # quick sources
        for workers in [1, 5]:
            yield ExampleSource(
                mode='subprocess',
                quick=True,
                parallel_workers=workers,
                string="workers-test-{}".format(workers),
                int=5,
                float="Inf",
                list=[4,5,6],
                dict={'x':1, 'y':0}
            )

        # defaults
        yield ExampleSource(
            string="workers-test-{}".format(workers),
            int=5,
            float="Inf",
            list=[4,5,6],
            dict={'x':1, 'y':0}
        )


    def functions(self):
        """create possible function elements"""
        # various amounts of parallel workers
        for workers in [1, 10, 11]:
            # restart subprocess
            for limit in [1, 10, 11]:
                # generate subprocess elements
                yield ExampleFunction(
                    mode='subprocess',
                    parallel_workers=workers,
                    worker_limit=limit
                )

        # defaults
        yield ExampleFunction()


    def test_variation(self):
        """test various combinations of pipe elements"""
        for src in self.sources():
            for func in self.functions():
                r = src.flow()
                r = func.flow(r)
                length = 0
                for record in r:
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
                    length += 1

                self.assertEqual(
                    length,
                    10*src.parallel_workers,
                    msg="{} -> {} (got {} instead of {} records)".format(
                        src, func, length, 10*src.parallel_workers
                    )
                )


                #sys.stderr.write(".")
                #sys.stderr.flush()


