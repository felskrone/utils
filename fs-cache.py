#!/usr/bin/python
'''
The Updater takes any function with parameters and executes them
various times within a minute. See the __main__ section for how to
add a function to execute to it.

The Updater would sit inbetween a process/object calls methods/functions
that do expensive operations regularly, for example the MWorker querying
CkMinions for connected_ids(). CkMinions.conncted_ids() currently traverses
the whole grains-cache in every execution producing a lot of i/o in larger
setups.
'''

import salt.config
import salt.utils.minions
import time
from multiprocessing import Queue, Process
import multiprocessing
import Queue
import os

class Updater(multiprocessing.Process):
    '''
    A class to execute functions in a cron like fashion
    but various within a minute, not every minute.
    '''

    m_queue = multiprocessing.Queue()
    workers = []
    funcs = {}

    values = {}

    def __init__(self, opts, queue):
        self.e_queue = queue
        self.opts = opts
        super(Updater, self).__init__()

    def add_update(self, name, **kwargs):
        '''
        adds a new function to the updater

        kwargs = {'fun' : func_ref,
                  'args' : [],
                  'tgt' : tgt_name in values,
                  'ival' : the interval}
        '''
        self.funcs[name] = {}
        self.funcs[name].update(kwargs)

    def run_update(self, **kwargs):
        '''
        execute a function with paramters, kwargs
        and passing on the queue to return the data to
        '''
        queue_args = {'tgt': kwargs['tgt'], 'queue': self.m_queue}
        proc = Process(target=kwargs['fun'], args=(kwargs['args']),kwargs=queue_args)
        proc.start()
        self.workers.append(proc)
        print "running {0}".format(kwargs)

    def run(self):
        '''
        main loop of the updater where all queue
        communication takes place
        '''
        timer = 1
        while timer > -1:
            for item in self.funcs:
                if timer in self.funcs[item]['ival']:
                    self.run_update(**self.funcs[item])

            cleaners = self.workers[:]
            try:
                item = self.m_queue.get(block=False)
                if isinstance(item, dict):
                    self.values.update(item)
            except Queue.Empty:
                pass
            for worker in enumerate(cleaners):
                if worker[1].is_alive():
                    continue
                else:
                    worker[1].join()
                try:
                    del self.workers[worker[0]]
                except Exception:
                    print "FAIL TO DEL WORKER"

            self.e_queue.put(self.values)

            timer += 1
            if timer >= 60:
                timer = 1
            time.sleep(0.1)


if __name__ == '__main__':

    opts = salt.config.master_config('/etc/salt/master')
    ckminions = salt.utils.minions.CkMinions(opts)

    # create a queue to talk to the updater, this would later
    # be the MWorker which queries the updater whenever necessary
    # to receive the cached data
    queue = multiprocessing.Queue()
    upd = Updater(opts, queue)

    # add to fs-expensive jobs to the updater
    # fun: reference to the function that should be executed by the updater
    # args: the parameters to the function that should be called in order(!)
    # tgt: the name of the key in the updates cache for the job
    # ival: the seconds within a minute on which to execute the run
    upd.add_update('grains_ids', **{'fun': ckminions.connected_ids,
                                    'args': [None, False],
                                    'tgt': 'grains_ids',
                                    'ival': [12, 32, 53]})
    upd.add_update('all_auth', **{'fun': ckminions._all_minions,
                                  'args': [None],
                                  'tgt': 'all_auth',
                                  'ival': [2, 22, 42]})
    upd.start()

    # consume the queues, that would be the MWorker using 
    # the cached data from the updater which is a zillion
    # times faster then doing it on the fs
    try:
        while 1:
            try:
                values = queue.get(block=False)
                if values is not None:
                    print "############"
                    for key, value in values.items():
                        print key, len(value)
                    print "############"
            except Queue.Empty:
                continue
            time.sleep(0.1)

    except KeyboardInterrupt:
        upd.terminate()
        upd.join()
