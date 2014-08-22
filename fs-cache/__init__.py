'''
Classes for salts filesystem cache for larger installations.
'''
import salt.utils

import salt.config
import time
import multiprocessing
import random
import sys
import zmq
from fsworker import FSWorker
from threading import Thread, Timer, Event

DEBUG = False


class FSTimer(Thread):
    '''
    A basic timer class the fires timer-events every second.
    '''
    def __init__(self, event):
        Thread.__init__(self)
        self.stopped = event
        self.daemon = True
        self.serial = salt.payload.Serial('msgpack')

    def run(self):
        '''
        main loop that fires the event every second
        '''
        context = zmq.Context()
        # the socket for outgoing timer events
        socket = context.socket(zmq.PUSH)
        socket.setsockopt(zmq.LINGER, 100)
        socket.bind("ipc:///tmp/fsc_timer")

        count = 0
        while not self.stopped.wait(1):
            socket.send(self.serial.dumps(count))

            count += 1
            if count >= 60:
                count = 0


class FSCache(multiprocessing.Process):
    '''
    Provides access to the cache-system and manages the subprocesses
    that do the cache updates in the background.

    Access to the cache is available to any module that connects
    to the FSCaches IPC-socket.
    '''

    def __init__(self, opts):
        '''
        starts the timer and inits the cache itself
        '''
        super(FSCache, self).__init__()
        # the possible settings for the cache
        self.opts = opts

        # all jobs the FSCache should run in intervals
        self.jobs = {}
        # the actual cached data
        self.path_data = {}

        # the timer provides 1-second intervals to the loop in run()
        # to make the cache system most responsive, we do not use a loop-
        # delay which makes it hard to get 1-second intervals without a timer
        self.timer_stop = Event()
        self.timer = FSTimer(self.timer_stop)
        self.timer.start()

    def add_job(self, **kwargs):
        '''
        adds a new job to the FSCache
        '''
        req_vars = ['name', 'path', 'ival', 'patt']

        # make sure new jobs have all variables set
        for var in req_vars:
            if var not in kwargs:
                raise AttributeError, "missing variable {0}".format(var)
        job_name = kwargs['name']
        del kwargs['name']
        self.jobs[job_name] = {}
        self.jobs[job_name].update(kwargs)

    def run_job(self, name):
        '''
        Creates a new subprocess to execute the given job in
        '''
        sub_p = FSWorker(**self.jobs[name])
        sub_p.start()

    def run(self):
        '''
        Main loop of the FSCache, checks schedule, retrieves result-data
        from the workers and answer requests with data from the cache
        '''
        context = zmq.Context()
        # the socket for incoming cache requests
        creq_in = context.socket(zmq.REP)
        creq_in.setsockopt(zmq.LINGER, 100)
        creq_in.bind("ipc:///tmp/fsc_cache")

        # the socket for incoming cache-updates from workers
        cupd_in = context.socket(zmq.REP)
        cupd_in.setsockopt(zmq.LINGER, 100)
        cupd_in.bind("ipc:///tmp/fsc_upd")

        # the socket for the timer-event
        timer_in = context.socket(zmq.PULL)
        timer_in.setsockopt(zmq.LINGER, 100)
        timer_in.connect("ipc:///tmp/fsc_timer")

        poller = zmq.Poller()
        poller.register(creq_in, zmq.POLLIN)
        poller.register(cupd_in, zmq.POLLIN)
        poller.register(timer_in, zmq.POLLIN)

        # our serializer
        serial = salt.payload.Serial("msgpack")

        while True:

            # we check every 10ms for new events on any socket
            socks = dict(poller.poll())

            # check for next cache-request
            if socks.get(creq_in) == zmq.POLLIN:
                msg = serial.loads(creq_in.recv())
                if DEBUG:
                    print "FSCACHE:  request {0}".format(msg)

                # we only accept requests as lists [req_id, <path>]
                if isinstance(msg, list):
                    # for now only one item is assumed to be requested
                    msgid, file_n = msg[:]
                    if DEBUG:
                        print "FSCACHE:  looking for {0}:{1}".format(msgid, file_n)

                    fdata = self.path_data.get(file_n, None)

                    if DEBUG:
                        if fdata is not None:
                            print "FSCACHE:  hit"
                        else:
                            print "FSCACHE:  miss"

                    # simulate slow caches
                    #randsleep = random.randint(0,3)
                    #time.sleep(randsleep)

                    # Send reply back to client
                    reply = serial.dumps([msgid, fdata])
                    creq_in.send(reply)

                # wrong format, item not cached
                else:
                    reply = serial.dumps([msgid, None])
                    creq_in.send(reply)

            # check for next cache-update from workers
            elif socks.get(cupd_in) == zmq.POLLIN:
                new_c_data = serial.loads(cupd_in.recv())
                # tell the worker to exit
                cupd_in.send(serial.dumps('OK'))

                # check if the returned data is usable
                if not isinstance(new_c_data, dict):
                    if DEBUG:
                        print "FSCACHE:  got unusable worker result"
                    del new_c_data
                    continue

                # the workers will return differing data:
                # 1. '{'file1': <data1>, 'file2': <data2>,...}' - a cache update
                # 2. '{search-path: None}' -  job was not run, pre-checks failed
                # 3. '{}' - no files found, check the pattern if defined?
                # 4. anything else is considered malformed

                if len(new_c_data) == 0:
                    if DEBUG:
                        print "FSCACHE:  got empty update from worker:"
                elif new_c_data.values()[0] is not None:
                    if DEBUG:
                        print "FSCACHE:  got cache update: {0}".format(len(new_c_data))
                    self.path_data.update(new_c_data)
                else:
                    if DEBUG:
                        print "FSCACHE:  got malformed result dict from worker"
                if DEBUG:
                    print "FSCACHE:  {0} entries".format(len(self.path_data))

            # check for next timer-event to start new jobs
            elif socks.get(timer_in) == zmq.POLLIN:
                sec_event = serial.loads(timer_in.recv())
                if DEBUG:
                    print "FSCACHE:  event: #{0}".format(sec_event)

                # loop through the jobs and start if a jobs ival matches
                for item in self.jobs:
                    if sec_event in self.jobs[item]['ival']:
                        self.run_job(item)

if __name__ == '__main__':

    opts = salt.config.master_config('./master')

    wlk = FSCache(opts)
    # add two jobs for jobs and cache-files
    wlk.add_job(**{
                    'name': 'grains',
                    'path': '/var/cache/salt/master/minions',
                    'ival': [2,12,22],
                    'patt': '^.*$'
                  })

    wlk.add_job(**{
                    'name': 'mine',
                    'path': '/var/cache/salt/master/jobs/',
                    'ival': [4,14,24,34,44,54],
                    'patt': '^.*$'
                 })
    wlk.start()
