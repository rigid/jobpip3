"""pipe element class"""

import os
import sys
import json
#import select
import traceback
import threading
from subprocess import Popen, PIPE, STDOUT

try: from Queue import Queue, Empty, Full # python 2.x
except ImportError: from queue import Queue, Empty, Full  # python 3.x

from record import Record




class Element(object):
    """a pipe element must implement the worker() method.
       The flow() function will serve as a generator wrapper
       for the worker() method that does the actual job.
       The worker() method can run as normal generator (mode='internal')
       or as (multiple) subprocesses (mode='subprocess') or on
       (multiple) remote hosts mode='remote'

       child classes will:
           - set the InRecord and OutRecord class attribute to define the type
             of records to process/produce (default: record.Record)
           - implement a worker() method that generates and/or takes records"""

    # set to True when we run on a POSIX system
    IS_POSIX = 'posix' in sys.builtin_module_names


    def __init__(self,
                 parallel_workers=1,
                 worker_limit=0,
                 mode='internal',
                 *args,
                 **kwargs):
        """:param parallel_workers: launch this many parallel workers
           :param worker_limit: if >0, restart subprocess after n input or
                output records. Elements without inputs are not restarted but
                just cut off.
           :param mode:
                - 'internal' to run element as normal generator (parallel_workers
                will always be 1)
                - 'subprocess' fork subprocess(es)"""

        # amount of processes to fork()
        self.parallel_workers = parallel_workers
        if parallel_workers <= 0:
            raise ValueError("parallel_workers must be > 0")
        # restart subprocess after it has processed this many input records
        self.worker_limit = worker_limit
        # mode we are running in (internal, subprocess, ...)
        self.mode = mode
        # class of input records
        self.InRecord = Record
        # class of output records
        self.OutRecord = Record
        # count input records
        self.input_count = 0
        # count output records
        self.output_count = 0
        # queues to pass records from/to subprocesses
        self._inqueue = Queue()
        self._outqueue = Queue()
        # thread that feeds records into inqueue
        self._feeder = None
        # (parallel) subprocesses returned by Popen()
        self._workers = []
        # current process-id
        self._worker_id = 0
        # args
        self._args = args
        self._kwargs = kwargs


    def __str__(self):
        return "<{}(parallel_workers={}, worker_limit={}, mode={}) object>".format(
            self.__class__.__name__,
            self.parallel_workers,
            self.worker_limit,
            self.mode
        )


    @staticmethod
    def _excepthook(type, value, tb):
        """hook that will be called when a subprocess raises an exception"""
        # output exception
        print >> sys.stderr, "exception in subprocess {}".format(os.getpid())
        print >> sys.stderr, traceback.format_exception(type, value, tb)
        # exit with error
        exit(os.EX_SOFTWARE)


    def _write_thread(self, process):
        """write records from input-queue to subprocess"""
        thread = threading.currentThread()
        #~ print >>sys.stderr, "{}: started".format(thread.name)

        # count records of this thread
        records_per_thread = 0
        # keep writing records
        while True:
            try:
                # get record from queue (the main feeder thread put it there)
                record = self._inqueue.get(timeout=0.1)
                #~ print >>sys.stderr, "{}: record inqueue -> stdin".format(
                    #~ thread.name
                #~ )
                # write to stdin of subprocess
                record.write(process.stdin)
                process.stdin.flush()
                # count input records for all threads
                self.input_count += 1
                # count output records for this thread
                records_per_thread += 1
                # release queue slot
                self._inqueue.task_done()
                # check limit ?
                if self.worker_limit <= 0:
                    # no limit set
                    continue

                # limit reached ?
                if self.worker_limit <= records_per_thread:
                    #~ print >>sys.stderr, "{} reached limit: {}".format(
                        #~ thread.name, self.worker_limit
                    #~ )
                    break

            # no new input records
            except Empty:
                #~ print >>sys.stderr, "{}: inqueue empty".format(
                    #~ thread.name
                #~ )
                # is input feeder still alive?
                if not self._feeder.is_alive():
                    # no more input - exit writer
                    break

        #~ print >>sys.stderr, "{}: exited (in: {})".format(
            #~ thread.name, records_per_thread
        #~ )
        process.stdin.flush()
        process.stdin.close()


    def _read_thread(self, process):
        """read records from subprocess and write to output-queue"""
        thread = threading.currentThread()
        #~ print >>sys.stderr, "{}: started".format(thread.name)

        # count records of this thread
        records_per_thread = 0
        # read one record per line from subprocess
        for record in self.OutRecord.read(process.stdout):
            # put newly read record into queue
            self._outqueue.put(record)
            #~ print >>sys.stderr, "{}: record stdout -> outqueue".format(
                #~ thread.name
            #~ )
            # count output records for all threads
            self.output_count += 1
            # count output records for this thread
            records_per_thread += 1

        #~ print >>sys.stderr, "{}: exited (out: {})".format(
            #~ thread.name, records_per_thread
        #~ )
        process.stdout.close()


    def _feed_thread(self, records):
        """feed records to subprocess queue of this element"""
        thread = threading.currentThread()
        #~ print >>sys.stderr, "{}: started".format(thread.name)

        # count records of this thread
        records_per_thread = 0
        # keep feeding
        for record in records:
            # put another record in the queue
            self._inqueue.put(record)
            #~ print >>sys.stderr, "{}: record iterable -> inqueue".format(
                #~ thread.name
            #~ )
            records_per_thread += 1

        #~ print >>sys.stderr, "{}: exited (records in: {})".format(
            #~ thread.name, records_per_thread
        #~ )


    def _launch_worker(self):
        """launch one subprocess"""

        # process descriptor, holding all info of a subprocess
        p = {
            'id' : self._worker_id,
            'process' : None,
            'reader' : None,
            'writer' : None
        }


        # start subprocess
        p['process'] = Popen(
            [
                # python interpreter
                sys.executable,
                # ourself with __name__ == __main__
                __file__,
                # name of element module
                self.__class__.__module__.split('.')[-1],
                # element class name
                self.__class__.__name__,
                # incremental id of this process
                "{}".format(self._worker_id),
                # record limit
                "{}".format(self.worker_limit),
                # args
                json.dumps(self._args),
                # kwargs
                json.dumps(self._kwargs)

            ],
            stdin=PIPE, stdout=PIPE, stderr=sys.stderr,
            close_fds=Element.IS_POSIX,
        )

        # start thread to read result-records from subprocesses
        p['reader'] = threading.Thread(
            name="{} reader thread: {}".format(
                self.__class__.__name__, self._worker_id
            ),
            target=self._read_thread,
            args=(p['process'],)
        )
        # launch
        p['reader'].start()

        # start thread to write input-records to subprocess
        if self._has_input:
            p['writer'] = threading.Thread(
                name="{} writer thread: {}".format(
                    self.__class__.__name__, self._worker_id
                ),
                target=self._write_thread,
                args=(p['process'],)
            )
            #launch
            p['writer'].start()

        # new id
        self._worker_id += 1

        return p


    def _flow_subprocess(self, records):
        """manage flow of element by forking subprocesses"""

        # start feeder thread (feeds records from iterable to queue)
        if self._has_input:
            self._feeder = threading.Thread(
                name="{} feeder thread".format(self.__class__.__name__),
                target=self._feed_thread,
                args=(records,)
            )
            self._feeder.start()

        # launch subprocesses
        for i in xrange(self.parallel_workers):
            self._workers += [ self._launch_worker() ]

        # keep yielding result records and monitor worker subprocesses
        while True:
            try:
                record = self._outqueue.get(timeout=0.1)
                # collect result records
                #~ print >>sys.stderr, "{}: record outqueue -> iterable".format(
                    #~ self.__class__.__name__
                #~ )
                # return record
                yield record
                # release queue slot
                self._outqueue.task_done()

            # currently no output records in queue
            except Empty:
                #~ print >>sys.stderr, "{}: outqueue empty".format(
                    #~ self.__class__.__name__
                #~ )
                pass

            # maintain all subprocess workers
            for p in self._workers:
                # was all input processed?
                if p['writer'] is not None and p['writer'].is_alive():
                    # nothing to be done
                    #~ print >>sys.stderr, "{}: writer still alive".format(
                        #~ p['writer'].name
                    #~ )
                    continue

                # is reader still alive?
                if p['reader'].is_alive():
                    continue

                # is worker still alive?
                if p['process'].poll() is None:
                    # nothing to be done for this worker
                    continue

                # unregister this worker
                #~ print >>sys.stderr, "{}: subprocess {} exited ({})".format(
                    #~ self.__class__.__name__, p['id'], p['process'].returncode
                #~ )
                self._workers.remove(p)

                # relaunch process if a worker_limit is set and we still get
                # input-data
                if self._has_input and self.worker_limit > 0 and \
                    (self._feeder.is_alive() or not self._inqueue.empty()):

                    #~ print >>sys.stderr, "{}: restarting subprocess {}".format(
                        #~ self.__class__.__name__, p['id']
                    #~ )
                    # launch new worker
                    self._workers += [ self._launch_worker() ]

            # result records left in outqueue?
            if not self._outqueue.empty():
                continue

            # no jobs left?
            if len(self._workers) == 0:
                # exit
                #~ print >>sys.stderr, "{}: no more subprocesses: exiting".format(
                    #~ self.__class__.__name__
                #~ )
                break

        #~ print >>sys.stderr, "{}: exited (in: {} out: {})".format(
            #~ self.__class__.__name__, self.input_count, self.output_count
        #~ )

        # block until all tasks are done
        self._outqueue.join()
        self._inqueue.join()


    def _flow_internal(self, records):
        """simple pass through of input/output records with limit checking"""

        def input_wrapper(records):
            """counting generator wrapper"""
            for record in records:
                # check type
                if record['__classname__'] != self.InRecord.__name__:
                    raise TypeError("Got {} as input but expected {}".format(
                        record['__classname__'], self.InRecord.__name__
                    ))
                # count input records
                self.input_count += 1
                # return record
                yield record

        # is worker_limit set?
        if self.worker_limit > 0:
            raise ValueError("worker_limit != 0 is not supported with mode='internal'")

        # process all records
        for record in self.worker(input_wrapper(records)):
            # check type
            if record['__classname__'] != self.OutRecord.__name__:
                raise TypeError("Got {} as output but expected {}".format(
                    record['__classname__'], self.OutRecord.__name__
                ))
            # return record
            yield record
            # count output records
            self.output_count += 1


    def flow(self, records=None):
        """generator that yields records from this element and/or passes
           records to it for processing them
           :param records: input records (optional)"""

        # got input records?
        if records is None: self._has_input = False
        else: self._has_input = True

        # how should we run?
        if self.mode == 'internal':
            # use parallel threads ?
            if self.parallel_workers > 1:
                raise NotImplementedError(
                    "{}: parallel_workers={} and threading with mode='internal'"
                    "not supported, yet".format(
                        self.__class__.__name__, self.parallel_workers
                    )
                )
            # no parallelization
            return self._flow_internal(records)

        # fork subprocess
        elif self.mode == 'subprocess':
            return self._flow_subprocess(records)

        # unknwon mode
        else:
            raise NotImplementedError("Unknown mode: {}".format(self.mode))


    def worker(self, records):
        """wrap actual element worker method - implemented in super(Element)"""

        raise NotImplementedError(
            """class "{}" needs a _wrapper() method""".format(
                self.__class__.__name__
            )
        )



# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import importlib

    def input_wrapper(element):
        """counting generator wrapper"""
        for record in element.InRecord.read(sys.stdin):
            # check type
            if record['__classname__'] != element.InRecord.__name__:
                raise TypeError("Got {} as input but expected {}".format(
                    record['__classname__'], element.InRecord.__name__
                ))
            # return record
            yield record
            # check limit ?
            if element.worker_limit <= 0: continue
            # limit reached ?
            if element.worker_limit <= element.input_count: break


    # set exception hook to handle exceptions in this subprocess
    sys.excepthook = Element._excepthook

    # the module to import the element class from
    element_module = importlib.import_module(sys.argv[1])
    # element class
    element_class = getattr(element_module, sys.argv[2])
    # incremental id of this subprocess
    worker_id = int(sys.argv[3])
    # limit amount of records for this subprocess
    record_limit = int(sys.argv[4])
    # arguments for elements
    element_args = json.loads(sys.argv[5])
    element_kwargs = json.loads(sys.argv[6])

    #~ print >>sys.stderr, "{} ({}): started. argv: {}".format(
        #~ element_class.__name__, worker_id, sys.argv
    #~ )

    # create element
    e = element_class(mode='internal', *element_args, **element_kwargs)
    # input records ?
    if e.InRecord is None:
        # element doesn't take input
        input_records = None
    else:
        # generator that reads record from stdin
        input_records = input_wrapper(e)


    # yield records from element
    for r in e.flow(input_records):
        # check type
        if r['__classname__'] != e.OutRecord.__name__:
            raise TypeError("Got {} as output but expected {}".format(
                r['__classname__'], e.OutRecord.__name__
            ))
        # write record to parent process
        r.write(sys.stdout)
        # flush once per record (so next element gets it immediately)
        sys.stdout.flush()

    #~ print >>sys.stderr, "{} ({}): exiting. records in: {}, out: {}".format(
        #~ element_class.__name__, worker_id, e.input_count, e.output_count
    #~ )

