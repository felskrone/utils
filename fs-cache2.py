#!/usr/bin/python
'''
The FSWalker-Class walks takes a path, a pattern and an interval. It then
traverses the given path in the given interval and filters files by the
given pattern.

While running, it continuesly updates a dict with all found files and
their data for which it can be queried for on demand.
'''
import salt.utils
import time
from multiprocessing import Queue
import multiprocessing
import Queue
import os
import stat
from re import match as rematch

class Statwalker(object):
    '''
    iterator class that walks through directory and
    collects the stat()-data for every file find
    '''

    def __init__(self, directory):
        self.stack = [directory]
        self.files = []
        self.index = 0

    def __getitem__(self, index):
        '''
        make it iterable
        '''
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
    Runs through a given directory and searches for files with the given pattern.
    If the pattern matches, reads the file and adds the data to a dictionary which
    is returned to the caller once done.
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
        if self.verify():
            print "running in dir {0}".format(self.path)
            for fn, _ in Statwalker(self.path):
                # add a few more checks data:
                # - dont open empty files
                # - what to add to the dict for empty files?
                if rematch(self.pattern, fn):
                    data[fn] = {}
                    data[fn] = salt.utils.fopen(fn, 'rb').read()
            # send the data back to the caller
            self.queue.put({self.name: data})


class FSWalker(multiprocessing.Process):
    '''
    A class to walk a given path and collect information in subprocesses
    '''
    # all jobs the FSWalker should run in intervals
    jobs = {}
    # the data collected from the paths
    path_data = {}
    # the subprocesses created to collect data
    workers = []
    # the queue the subprocess write their results into
    result_q = multiprocessing.Queue()

    def __init__(self):
        super(FSWalker, self).__init__()

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
        print self.jobs

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
            print "timer: {0}   workers: {1}".format(timer, self.workers)

            # loop through the jobs and start if a jobs ival matches
            for item in self.jobs:
                if timer in self.jobs[item]['ival']:
                    self.run_job(item)

            # get new data from the queue if present
            try:
                self.path_data.update(self.result_q.get(block=False))
            except Queue.Empty:
                pass

            # print what is currently cached
            print "Files in Cache:",
            for data in self.path_data:
                print "{0}:{1} ".format(data, len(self.path_data[data])),
            print ""


            # make sure all workers are joined properly and
            # remove them is the are done with their job
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

    wlk = FSWalker()

    wlk.add_job(**{
                    'name': 'grains',
                    'path': '/var/cache/salt/master/minions',
                    'ival': [2,12,22,32,42,52],
                    'patt': '^.*/data.p$'
                  })

    wlk.add_job(**{
                    'name': 'mine',
                    'path': '/var/cache/salt/master/minions',
                    'ival': [6,16,26,36,46,56],
                    'patt': '^.*/mine.p$'
                  })

    wlk.start()

    itera = 0
    try:
        while 1:
            time.sleep(0.5)

    except KeyboardInterrupt:
        wlk.terminate()
        wlk.join()
