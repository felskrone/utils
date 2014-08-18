#!/usr/bin/python
'''
Classes for caching files and their data to reduce i/o.
'''
import salt.utils
import time
from multiprocessing import Queue
import multiprocessing
import Queue
import os
import stat
from re import match as rematch
import random
import sys

class Statwalker(object):
    '''
    iterator class that walks through directory and
    collects the stat()-data for every file it finds
    '''

    def __init__(self, directory):
        self.stack = [directory]
        self.files = []
        self.index = 0

    def __getitem__(self, index):
        '''
        make it iterable
        '''
        count = 0
        while 1:

            try:
                fn = self.files[self.index]
                self.index = self.index + 1
            except IndexError:
                # pop next directory from stack
                self.directory = self.stack.pop()
                try:
                    self.files = os.listdir(self.directory)
                    self.index = 0
                except OSError as _:
                    print "Folder not found... %s" % (self.directory)
            else:
                # got a filename
                fullname = os.path.join(self.directory, fn)
                st = os.stat(fullname)
                mode = st[stat.ST_MODE]
                if stat.S_ISDIR(mode) and not stat.S_ISLNK(mode):
                    self.stack.append(fullname)
                return fullname, st

class Job(multiprocessing.Process):
    '''
    Runs through a given directory once and searches for files with the given pattern.
    If the pattern matches, reads the file and adds the data to a dictionary which
    is returned to the caller once done. It then exits.
    '''

    def __init__(self, name, res_q, **kwargs):
        super(Job, self).__init__()
        self.path = kwargs['path']
        self.pattern = kwargs['patt']
        self.queue = res_q
        self.name = name

    def verify(self):
        '''
        make sure target-path is an actual dir
        '''
        if os.path.isdir(self.path):
            return True

    def run(self):
        '''
        main loop that searches directories and retrieves the data
        '''
        data = {}
        dir_n = os.path.dirname
        if self.verify():
            print "WORKER:  running in dir {0}".format(self.path)
            for fn, _ in Statwalker(self.path):
                # add a few more checks data:
                # - dont open empty files
                # - what to add to the dict for empty files?
                if rematch(self.pattern, fn):
                    data[fn] = {}
                    data[fn] = 'test'
                    #data[fn] = salt.utils.fopen(fn, 'rb').read()
            # send the data back to the caller
            self.queue.put({self.name: data})


class FSWalker(multiprocessing.Process):
    '''
    A manager-clas that spawns subprocess that walk through a given path to
    collect files and their data to put them into cache
    '''
    # all jobs the FSWalker should run in intervals
    jobs = {}
    # the data collected from the paths
    path_data = {}
    # the subprocesses created to collect data
    workers = []
    # the queue the subprocess write their results into
    result_q = multiprocessing.Queue()

    def __init__(self, in_q=None, out_q=None):
        super(FSWalker, self).__init__()
        # the FSWalker communication queues
        self.in_queue = in_q
        self.out_queue = out_q

    def add_job(self, **kwargs):
        '''
        adds a new job to the FSWalker
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
        creates a new subprocess to execute the given job in
        '''
        job = Job(name, self.result_q, **self.jobs[name])
        job.start()
        self.workers.append(job)

    def run(self):
        '''
        main loop of the FSWalker, checks schedule, retrieves result-data
        and make sure all subprocess workers are properly joined
        '''
        timer = 1
        while timer > -1:
#            print "timer: {0}   workers: {1}".format(timer, self.workers)

            # loop through the jobs and start if a jobs ival matches
            for item in self.jobs:
                if timer in self.jobs[item]['ival']:
                    self.run_job(item)

            # get new data from the queue if present
            try:
                self.path_data.update(self.result_q.get(block=False))
            except Queue.Empty:
                pass

            # check if someone asked for data
            try:
               req = self.in_queue.get(block=False)
               if isinstance(req, dict):
                   # for now only one item is assumed
                   type_n, file_n = req.items()[0]
                   print "CACHE:  looking for {0}:{1}".format(type_n, file_n)

                   dir_data = self.path_data.get(type_n, False)

                   if dir_data is not None:
                       f_data = self.path_data[type_n].get(file_n, None)
                       self.out_queue.put(f_data)
                       
            except Queue.Empty:
                pass

            # print what is currently cached
            print "CACHE:  Files in Cache:",
            for data in self.path_data:
                print "{0}:{1} ".format(data, len(self.path_data[data])),
            print ""


            # make sure all workers are joined properly and
            # remove them if they are done with their job
            for worker in enumerate(self.workers):
                if worker[1].is_alive():
                    continue
                try:
                    worker[1].join()
                    del self.workers[worker[0]]
                except Exception:
                    print "FAIL TO DEL WORKER"

            timer += 1
            if timer >= 60:
                timer = 1
            time.sleep(1)


if __name__ == '__main__':
    # the communication queues for querying the FSWalkers cache
    p_queue = multiprocessing.Queue()
    g_queue = multiprocessing.Queue()

    wlk = FSWalker(in_q=p_queue, out_q=g_queue)
    grains_path = '/var/cache/salt/master/minions/'
    mine_path = '/var/cache/salt/master/minions/'
    minion = sys.argv[1]

    # add two jobs for mine and grains data
    wlk.add_job(**{
                    'name': 'grains',
                    'path': '/var/cache/salt/master/minions',
                    'ival': [2,12,22,32,42,52],
                    'patt': '^.*/data.p$'
                  })

    wlk.add_job(**{
                    'name': 'mine',
                    'path': '/var/cache/salt/master/minions',
                    'ival': [4,14,24,34,44,54],
                    'patt': '^.*/mine.p$'
                 })

    wlk.start()

    counter = 0
    try:
        print "MAIN:  main: waiting 5 seconds for cache to populate..."
        time.sleep(5)
        # ask the cache for grains data
        print "MAIN:  query cache with grains for host {0}".format(sys.argv[1])
        p_queue.put({'grains': grains_path + minion + '/data.p'})
        try:
            print "MAIN: cache reply: {0} ".format(g_queue.get())
        except Queue.Empty:
            pass
        while 1:
            time.sleep(0.5)

    except KeyboardInterrupt:
        wlk.terminate()
        wlk.join()
