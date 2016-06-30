"""pipe element class"""

import os
import sys
import json
import time
import traceback
import threading
import setproctitle
from subprocess import Popen, PIPE, STDOUT

try: from Queue import Queue, Empty, Full  # python 2.x
except ImportError: from queue import Queue, Empty, Full  # python 3.x

from .util import log
from .records import Record




class Element(object):
    """a pipe element must implement the worker() method.

       The flow() function will serve as a generator wrapper
       for the worker() method that does the actual job.

       The worker() method can run as normal generator (mode='internal')
       or as (multiple) subprocesses (mode='subprocess') or on
       (multiple) remote hosts mode='remote'

       child classes will:
           - pass all arguments in serializable form to super(...).__init__(...)
           - set the InRecord and OutRecord class attribute to define the type
             of records to process/produce (default: record.Record)
           - implement a worker() method that generates and/or consumes
             records"""

    # set to True when we run on a POSIX system
    IS_POSIX = 'posix' in sys.builtin_module_names


    def __init__(self,
                 parallel_workers=1,
                 worker_limit=0,
                 mode='subprocess',
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
        # true, if we are running inside a subproccess
        self.is_subprocess = False
        # class of input records
        self.InRecord = Record
        # class of output records
        self.OutRecord = Record
        # count input records
        self.input_count = 0
        # count output records
        self.output_count = 0
        # queues to pass records from/to subprocesses
        self._inqueue = Queue(maxsize=self.parallel_workers*10)
        self._outqueue = Queue(maxsize=self.parallel_workers*10)
        # thread that feeds records into inqueue
        self._feeder = None
        # (parallel) subprocesses returned by Popen()
        self.workers = []
        # current process-id
        self._worker_id = 0

        # total amount of records seen by this processor (passed + dismissed)
        self.total = 0
        # amount of records that passed through this processor
        self.passed = 0
        # amount of records that were not passed on
        self.dismissed = 0
        # amount of records that were touched by this processor
        self.modified = 0

        # args
        self._args = args
        self._kwargs = kwargs


    def __str__(self):
        return "<{}({}parallel_workers={}, worker_limit={}, mode={}{}) object>".format(
            self.__class__.__name__,
            self._args + ", args=" if self._args != () else "",
            self.parallel_workers,
            self.worker_limit,
            self.mode,
            ", kwargs=" + unicode(self._kwargs) if self._kwargs != {} else ""
        )


    def _status(self):
        """print current state of this processor"""

        # calculate missing values
        if self.total == 0 and (self.passed != 0 or self.dismissed != 0):
            self.total = self.passed + self.dismissed
        elif self.passed == 0 and (self.total != 0 or self.dismissed != 0):
            self.passed = self.total - self.dismissed
        elif self.dismissed == 0 and (self.total != 0 or self.passed != 0):
            self.dismissed = self.total - self.passed

        log.info("{}: passed: {}, dismissed: {}, "
                 "modified: {}, total: {}".format(
                self.__class__.__name__,
                self.passed, self.dismissed,
                self.modified, self.total
            )
        )


    @staticmethod
    def _excepthook(type, value, tb):
        """hook that will be called when a subprocess raises an exception"""
        # output exception
        print >>sys.stderr, "LOG:exception in subprocess {}: {}".format(
            setproctitle.getproctitle(),
            traceback.format_exception(type, value, tb)
        )

        # exit with error
        exit(os.EX_SOFTWARE)


    def _input_record_writer(self, worker):
        """write records from input-queue to subprocess' stdin"""
        thread = threading.currentThread()
        log.noisy("{}: started".format(thread.name))

        record_count = 0

        # keep writing records while subprocess is alive
        while worker['process'].poll() is None:
            # break when limit is reached
            if self.worker_limit != 0 and record_count >= self.worker_limit:
                break

            # decrement amount of available unprocessed record
            # slots (or block until there's a free slot)
            worker['unprocessed'].acquire()

            try:
                # get record from queue (the main feeder thread put it there)
                record = self._inqueue.get(timeout=1.0)
                # release queue slot
                self._inqueue.task_done()
                log.verynoisy("{}: record inqueue -> stdin (unprocessed: {})".format(
                    thread.name, worker['unprocessed']
                ))
                # write to stdin of subprocess
                record.write(worker['process'].stdin)
                # count one more record
                record_count += 1
                # check limit ?
                if self.worker_limit <= 0:
                    # no limit set
                    continue
                # limit reached ?
                if self.worker_limit <= record_count:
                    log.debug("{} reached limit: {}".format(
                        thread.name, self.worker_limit
                    ))
                    break

            # no new input records
            except Empty:
                log.verynoisy("{}: inqueue empty".format(
                    thread.name
                ))
                # no feeder there (yet)
                if self._feeder is None: continue
                # is input feeder still alive?
                if not self._feeder.is_alive() and self._inqueue.empty():
                    # no more input - exit writer
                    break

        log.debug("{}: exited (in: {})".format(
            thread.name, record_count
        ))

        worker['process'].stdin.close()


    def _output_record_reader(self, worker):
        """read records from subprocess stdout and write to output-queue"""
        thread = threading.currentThread()
        log.noisy("{}: started".format(thread.name))

        # read one record per line from subprocess
        for record in self.OutRecord.read(worker['process'].stdout):
            # put newly read record into queue
            self._outqueue.put(record)
            log.verynoisy("{}: record stdout -> outqueue".format(
                thread.name
            ))

        log.noisy("{}: exited".format(thread.name))
        worker['process'].stdout.close()


    def _feedback_reader(self, worker):
        """read status & logging output from subprocess stderr"""

        thread = threading.currentThread()
        log.noisy("{}: started".format(thread.name))

        for line in iter(worker['process'].stderr.readline, ''):

            # lines from subprocess should be in the format "<keyword>:<payload"
            try: keyword, payload = line.split(":", 1)
            # log malformed input at error level
            except ValueError:
                log.error("{} ({}): \"{}\"".format(
                    self.__class__.__name__,
                    worker['process'].pid,
                    line.strip()
                ))
                continue

            # keyword is always uppercase
            keyword = keyword.upper()
            # strip payload string
            payload = payload.strip()

            # log msg ?
            if keyword == "LOG":
                # if we put it on stderr, it will be <level>|||<msg>
                if "|||" in payload:
                    level, msg = payload.split("|||", 1)
                    log.log(level, "{}: {}".format(
                        self.__class__.__name__,
                        msg
                    ))

                # unformatted log message
                else:
                    log.error(payload)

            # status msg ?
            elif keyword == "STATUS":
                # subprocess fetched another record from stdin
                if payload == "record read":
                    # decrement counter
                    worker['unprocessed'].release()
                else:
                    log.warn("got unknown STATUS from subprocess: \"{}\"".format(
                        payload
                    ))
            # if anything fails, print line as error
            else:
                log.error(line.strip())


    def _input_record_feeder(self, records):
        """feed records to input queue (all subprocesses of this element)"""
        thread = threading.currentThread()
        log.noisy("{}: started".format(thread.name))

        # keep feeding
        for record in records:
            # put another record in the queue
            self._inqueue.put(record)
            log.verynoisy("{}: record iterable -> inqueue".format(
                thread.name
            ))

            self.input_count += 1

        log.noisy("{}: exited (records in: {})".format(
            thread.name, self.input_count
        ))


    def _worker_maintainer(self):
        """maintain workers"""

        while True:

            # maintain all subprocess workers
            for w in self.workers:
                # was all input processed?
                if w['writer'] is not None and w['writer'].is_alive():
                    # nothing to be done
                    log.verynoisy("{}: writer still alive".format(
                        w['writer'].name
                    ))
                    continue

                # is reader still alive?
                if w['reader'] is not None and w['reader'].is_alive():
                    continue

                # is worker subprocess still alive?
                if w['process'].poll() is None:
                    # nothing to be done for this worker
                    continue

                # unregister this worker
                log.debug("{}: subprocess {} exited ({})".format(
                    self.__class__.__name__, w['id'], w['process'].returncode
                ))
                self.workers.remove(w)

                # relaunch process if a worker_limit is set and we still get
                # input-data
                if self.InRecord is not None and \
                    self.worker_limit > 0 and \
                    (self._feeder.is_alive() or not self._inqueue.empty()):

                    log.debug("{}: restarting subprocess {}".format(
                        self.__class__.__name__, w['id']
                    ))
                    # launch new worker
                    self.workers += [ self._launch_worker() ]

            # result records left in outqueue?
            if not self._outqueue.empty():
                continue

            # no jobs left?
            if len(self.workers) == 0:
                # exit
                log.debug("{}: no more subprocesses: exiting".format(
                    self.__class__.__name__
                ))
                break


    def _launch_worker(self):
        """launch one subprocess"""

        # worker descriptor, holding all info of a subprocess
        w = {
            # id of this worker subprocess
            'id': self._worker_id,
            # result of Popen
            'process': None,
            # _feedback_reader() thread of this subprocess
            'fedback': None,
            # _output_record_reader() thread
            'reader': None,
            # _input_record_writer() thread
            'writer': None,
            # amount of records written to stdin of this
            # subprocess but have not been read by it, yet
            'unprocessed': threading.BoundedSemaphore(5),
        }


        # start subprocess
        log.debug("{}: launching subprocess, cwd={}, args={}".format(
            self.__class__.__name__,
            os.path.split(os.path.dirname(__file__))[0],
            "[{}, {}, {}, {}, {}, {}]".format(
                # name of element module
                self.__class__.__module__,
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
            )
        ))

        w['process'] = Popen(
            [
                # python interpreter
                sys.executable,
                # run as module
                "-m",
                # ourself with __name__ == __main__
                __name__,
                # loglevel
                log.getLevel(),
                # name of element module
                self.__class__.__module__,
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
            # use line-buffering
            # bufsize=1,
            cwd=os.path.split(os.path.dirname(__file__))[0],
            stdin=PIPE, stdout=PIPE, stderr=PIPE,
            close_fds=Element.IS_POSIX,
        )

        # start thread to read status & logging output from subprocess
        w['feedback'] = threading.Thread(
            name="{} feedback reader thread: {}".format(
                self.__class__.__name__, self._worker_id
            ),
            target=self._feedback_reader,
            args=(w,)
        )
        # launch
        w['feedback'].start()

        # start thread to read result-records from subprocesses
        if self.OutRecord is not None:
            w['reader'] = threading.Thread(
                name="{} reader thread: {}".format(
                    self.__class__.__name__, self._worker_id
                ),
                target=self._output_record_reader,
                args=(w,)
            )
            # launch
            w['reader'].start()

        # start thread to write input-records to subprocess
        if self.InRecord is not None:
            w['writer'] = threading.Thread(
                name="{} writer thread: {}".format(
                    self.__class__.__name__, self._worker_id
                ),
                target=self._input_record_writer,
                args=(w,)
            )
            # launch
            w['writer'].start()

        # new id
        self._worker_id += 1

        return w


    def _flow_subprocess(self, records):
        """manage flow of element by forking subprocesses"""

        # start feeder thread (feeds records from iterable to queue)
        if self.InRecord is not None:
            self._feeder = threading.Thread(
                name="{} feeder thread".format(self.__class__.__name__),
                target=self._input_record_feeder,
                args=(records,)
            )
            self._feeder.start()

        # launch workers
        for i in xrange(self.parallel_workers):
            self.workers += [ self._launch_worker() ]

        # launch maintainer thread
        self._maintainer = threading.Thread(
            name="{} maintainer".format(self.__class__.__name__),
            target=self._worker_maintainer,
            args=()
        )
        self._maintainer.start()

        # keep yielding result records
        while self._maintainer.is_alive():

            # yield output records?
            if self.OutRecord is not None:
                try:
                    record = self._outqueue.get(timeout=1.0)
                    # release queue slot
                    self._outqueue.task_done()
                    log.verynoisy("{}: record outqueue -> generator".format(
                        self.__class__.__name__
                    ))
                    # return record
                    yield record
                    self.output_count += 1

                # currently no output records in queue
                except Empty:
                    log.verynoisy("{}: outqueue empty".format(
                        self.__class__.__name__
                    ))
                    pass

            # no need to yield output records, just wait for the maintainer
            # thread to die
            else:
                time.sleep(0.5)

        # block until all tasks are done
        self._outqueue.join()
        self._inqueue.join()

        # update stats
        self.total = self.input_count
        self.passed = self.output_count

        # status message
        self._status()


    def _flow_internal(self, records):
        """simple pass through of input/output records with limit checking"""

        def input_wrapper(records):
            """counting generator wrapper"""
            for record in records:
                # check if input records are of expected class
                if not isinstance(record, self.InRecord):
                    # convert it
                    record = self.InRecord(record)

                # count input records
                self.input_count += 1

                # return record
                yield record


        # is worker_limit set?
        if self.worker_limit > 0:
            raise ValueError("worker_limit != 0 is not supported with mode='internal'")

        # process all records
        for record in self.worker(input_wrapper(records)):
            # process output records ?
            if self.OutRecord is not None:
                # check if output records are of expected class
                if not isinstance(record, self.OutRecord):
                    # convert it
                    record = self.OutRecord(record)

                # return record
                yield record
                # count output records
                self.output_count += 1

        # status message
        self._status()


    def flow(self, records=None):
        """generator/method that yields records from this element and/or passes
           records to it for processing them. Generator when Element() produces
           records (self.OutRecord != None), method when not
           (self.OutRecord == None)
           :param records: input records (optional)"""


        # reset counters
        self.total = 0
        self.dismissed = 0
        self.passed = 0
        self.modified = 0

        # how should we run?
        if self.mode == 'internal':
            # use parallel threads ?
            if self.parallel_workers > 1:
                raise NotImplementedError(
                    "{}: parallel_workers={} and threading with mode='internal' "
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
    import os
    import importlib

    def input_wrapper(element):
        """counting generator wrapper"""
        count = 0
        for record in element.InRecord.read(sys.stdin):
            # notify parent process that another record has been read
            print >>sys.stderr, "STATUS:record read"
            # check type
            if not isinstance(record, element.InRecord):
                # convert it
                record = element.InRecord(record)
            # return record
            yield record
            count += 1
            # check limit ?
            if element.worker_limit <= 0: continue
            # limit reached ?
            if element.worker_limit <= count:
                log.debug("{} subprocess reached worker_limit " \
                    "({}). Exiting.".format(
                    element.__class__.__name__,
                        element.worker_limit
                    ))
                break


    # set exception hook to handle exceptions in this subprocess
    sys.excepthook = Element._excepthook
    # current loglevel of parent process
    loglevel = sys.argv[1]
    # the module to import the element class from
    element_module = importlib.import_module(sys.argv[2])
    # element class
    element_class = getattr(element_module, sys.argv[3])
    # incremental id of this subprocess
    worker_id = int(sys.argv[4])
    # limit amount of records for this subprocess
    record_limit = int(sys.argv[5])
    # arguments for elements
    element_args = json.loads(sys.argv[6])
    element_kwargs = json.loads(sys.argv[7])

    # set process title
    setproctitle.setproctitle("{}({}, {})".format(
        element_class.__name__,
        element_args,
        element_kwargs
    ))

    # initialize logging
    log.init(
        instance=element_class.__name__,
        stream=True,
        console=False,
        level=loglevel
    )
    # create element
    e = element_class(*element_args, mode='internal', **element_kwargs)
    # mark as running inside subprocess
    e.is_subprocess = True

    log.debug("subprocess {} ({}): started. argv: {}".format(
        element_class.__name__, worker_id, sys.argv
    ))

    # got no input records ?
    if e.InRecord is None:
        # element doesn't take input
        input_records = None
    else:
        # generator that reads record from stdin
        input_records = input_wrapper(e)


    # yield records from element
    for r in e.flow(input_records):
        # write result record to parent process
        r.write(sys.stdout)

    log.debug("{}: exiting. records in: {}, out: {}".format(
        setproctitle.getproctitle(), e.input_count, e.output_count
    ))

