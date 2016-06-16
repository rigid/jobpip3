#!/usr/bin/env python

"""simple pip3job information pipeline example"""






# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    from jobpip3.sources import ExampleSource
    from jobpip3.functions import ExampleFunction
    from jobpip3 import Pipe

    #~ # ------------------------------------------------------------------------
    #~ # default mode (simply use generator functions)
    #~ src = ExampleSource()
    #~ func = ExampleFunction()

    #~ records = src.well()
    #~ records = func.process(records)

    #~ for r in records:
        #~ r.write(sys.stdout)

    #~ print "----------------------------------------------------------"

    #~ # ------------------------------------------------------------------------
    #~ # create subprocesses
    #~ src = ExampleSource(mode='subprocess')
    #~ func = ExampleFunction(mode='internal')

    #~ records = src.well()
    #~ records = func.process(records)

    #~ for r in records:
        #~ r.write(sys.stdout)

    #~ print "----------------------------------------------------------"

    #~ # ------------------------------------------------------------------------
    #~ # create subprocesses + run in parallel
    #~ src = ExampleSource(mode='subprocess', parallel_jobs=5)
    #~ func = ExampleFunction(mode='subprocess', parallel_jobs=2)

    #~ records = src.well()
    #~ records = func.process(records)

    #~ for r in records:
        #~ r.write(sys.stdout)

    #~ print "----------------------------------------------------------"

    # ------------------------------------------------------------------------
    # create subprocesses + run in parallel + restart every 2 records

    src = ExampleSource(foo="bar", mode='subprocess')
    func = ExampleFunction(mode='subprocess', parallel_workers=1, worker_limit=1)

    records = src.flow()
    records = func.flow(records)

    for r in records:
        r.write(sys.stdout)

    print "----------------------------------------------------------"


    # ------------------------------------------------------------------------
    # Pipe test
    pipe = Pipe(ExampleSource(), [ ExampleFunction() ])

    for r in pipe.run():
        r.write(sys.stdout)

    print "----------------------------------------------------------"
