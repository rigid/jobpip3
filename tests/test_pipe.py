
import sys
import unittest
from jobpip3.source_example import ExampleSource
from jobpip3.function_example import ExampleFunction



class TestPip3(unittest.TestCase):
    """jobpip3 tests"""


    def sources(self):
        """create possible source elements"""
        # variations of mode='subprocess')
        for workers in [1, 5]:
            yield ExampleSource(mode='subprocess', parallel_workers=workers)

        # defaults
        yield ExampleSource()


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
                length = len(list(r))
                self.assertEqual(
                    length,
                    10*src.parallel_workers,
                    msg="{} -> {} (got {} instead of {} records)".format(
                        src, func, length, 10*src.parallel_workers
                    )
                )
                #sys.stderr.write(".")
                #sys.stderr.flush()


