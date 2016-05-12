
import sys
import unittest
from jobpip3.source_example import ExampleSource
from jobpip3.function_example import ExampleFunction



class TestPip3(unittest.TestCase):
    """jobpip3 tests"""


    def shuffle_src(self):
        """create possible source elements"""
        # variations of mode='subprocess')
        for workers in [1, 5]:
            yield ExampleSource(mode='subprocess', parallel_workers=workers)

        # defaults
        yield ExampleSource()


    def shuffle_func(self):
        """create possible function elements"""
        # 1 - 20 parallel jobs
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
        for src in self.shuffle_src():
            for func in self.shuffle_func():
                r = src.flow()
                r = func.flow(r)
                length = len(list(r))
                self.assertEqual(
                    length,
                    10*src.parallel_workers,
                    msg="{} -> {} ({} instead of {} records)".format(
                        src, func, length, 10*src.parallel_workers
                    )
                )
                sys.stderr.write(".")
                sys.stderr.flush()


