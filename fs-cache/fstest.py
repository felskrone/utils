#!/usr/bin/python
'''
Classes for salts filesystem cache for larger installations.
'''
import salt.utils
import salt.config
import time
import random
from random import shuffle
import sys
import zmq
from __init__ import FSCache
from fsworker import Statwalker
import argparse


class Argparser(object):

    def __init__(self):
        self.main_parser = argparse.ArgumentParser()
        self.add_args()

    def add_args(self):

        self.main_parser.add_argument('-f',
                                      type=str,
                                      default='',
                                      dest='pathfile',
                                      required=True,
                                      help='the file from which to load paths')

        self.main_parser.add_argument('-r',
                                      type=int,
                                      default=1,
                                      dest='runs',
                                      required=False,
                                      help='the number of runs to execute')

        self.main_parser.add_argument('-t',
                                      type=str,
                                      default='cache',
                                      dest='type',
                                      required=False,
                                      help='wether to run the tests on cache or filesystem')

    def parse_args(self):
        return self.main_parser.parse_args()


class FSCacheTest(object):
    '''
    run different tests for the FSCache
    '''

    def __init__(self, opts, runs=1, type='cache'):
        self.opts = opts
        self.runs = runs
        self.serial = salt.payload.Serial("msgpack")
        self.type = type

        # the cache timeout in ms * 5 is the max time we allow
        # the cache to take to reply to the request
        self.cache_timeout = 50

        self.dirs = [
                        '/var/cache/salt/master/jobs/',
                        '/var/cache/salt/master/minions/'
                    ]

        # the number of random files to load
        self.rand_files = 3000
        self.files = []
        self.filelist = 'filelist'
        self.setup()

        self.stats = {
                       'req_total' : 0,
                       'hits': 0,
                       'misses': 0,
                       'bytes': 0,
                       'avg_100': 0,
                       'avg_1000': 0,
                       'runs': 0
                     }

    def setup(self):
        self.context = zmq.Context()
        self.cache_cli = self.context.socket(zmq.REQ)
        self.cache_cli.connect('ipc:///tmp/fsc_cache')

        self.poll = zmq.Poller()
        self.poll.register(self.cache_cli, zmq.POLLIN)

    def do_cache_req(self, path):

        # add random id to cache-request to have a 1:1 relation
        msgid = random.randint(10000, 20000)
        msg = [msgid, path]
        self.stats['req_total'] += 1

        try:
            # send a request to the FSCache. If we get to here after
            # the cache was too slow before, the send() will fail but we try
            # anyway going by EAFP rules.
#            print "MAIN:  Query: {0}".format(msg)
            self.cache_cli.send(self.serial.dumps(msg))
        except zmq.error.ZMQError:
            # on failure we do a dummy recv()and resend the last msg. That
            # keeps us from having to re-init the socket on failure.
            self.cache_cli.recv()
            print "MAIN:  Resend: {0}:{1}".format(msgid, msg)
            self.cache_cli.send(self.serial.dumps(msg))

        to_max = self.cache_timeout*5
        to_count = 0

        while 1:
            # we wait cache_timout*5
            self.socks = dict(self.poll.poll(self.cache_timeout))
            if self.socks.get(self.cache_cli) == zmq.POLLIN:
                reply = self.serial.loads(self.cache_cli.recv())

                # no reply is broken, we brake the loop
                if not reply:
                    break

                # we expect to receive only lists with an id
                # and the data for the requested file
                if isinstance(msg, list):
                    # first field must be our matching request-id
                    if msgid == reply[0]:
                        if reply[1] is not None:
#                            print "MAIN:  Reply: {0}:{1} bytes".format(reply[0], len(reply[1]))
                            self.stats['hits'] += 1
                            self.stats['bytes'] += sys.getsizeof(reply[1])
                        else:
#                            print "MAIN:  Reply: {0}:miss".format(reply[0])
                            self.stats['misses'] += 1
                        #print ""
                        # received reply for request, enable the
                        # break the loop to make way for next request
                        break
                # None is a cache miss, we have to go to disk
                elif msg is None:
                    print "MAIN:  cache miss..."
                # we should never get here
                else:
                    print "MAIN:  deformed packet {0}".format(reply)
                    raise zmq.error.ZMQError, "invalid state in FSCache-communication"
            # we wait a maximum time of cache_timeout*5, which means 100ms
            #to_count += cache_timeout
            to_count += 1
            if to_count == to_max:
                to_count = 0
                print "MAIN:  cache was too slow, breaking\n"
                break

    def do_fs_req(self, path):
        '''
        open a single file from disk and read its data
        '''
        try:
            fhandle = open(path, 'rb')
            data = fhandle.read()
            #print data
            self.stats['hits'] += 1
            self.stats['bytes'] += sys.getsizeof(data)
            fhandle.close()
        except IOError:
            self.stats['misses'] += 1
            return

    def load_random(self):
        '''
        load filenames randomly from a filelist
        '''
        fhandle = open('./filelist', 'r')

        while not len(self.files) >= self.rand_files:
            for file_n in fhandle.readlines():
                rint = random.randint(1, 1000)
                if rint % 10 == 0:
                    self.files.append(file_n.strip())
                if len(self.files) >= self.rand_files:
                    break
                else:
                    fhandle.seek(0)
        shuffle(self.files)
        print "MAIN/{0}:  loaded {1} paths-strings".format(self.stats['runs'],
                                                           len(self.files))

    def do_random(self, num, cache=True):
        '''
        do cache requests with random paths
        '''
        count = 0
        t_start = time.time()
        for file_n in self.files[:num]:
            if not cache:
                self.do_fs_req(file_n)
            else:
                self.do_cache_req(file_n)
            count += 1

        t_stop = time.time()
        t_delta = t_stop - t_start

        print "MAIN/{0}:  did {1} requests in: {2}".format(self.stats['runs'],
                                                           count,
                                                           t_delta)
        return t_delta


    def do_fs(self, num):

        count = 0
        t_start = time.time()
        for file_n in self.files[:num]:
            count += 1

        t_stop = time.time()
        t_delta = t_stop - t_start

        print "MAIN/{0}:  did {1} requests in: {2}".format(self.stats['runs'],
                                                           count,
                                                           t_delta)
        return t_delta

    def pstats(self):
        print "Summary for {0} runs".format(self.stats['runs'])
        print "Requests: {0},  Hits: {1},  Misses: {2}".format(self.stats['req_total'],
                                                               self.stats['hits'],
                                                               self.stats['misses'])

        print "avg/100: {0},  avg/1000: {1},  bytes: {2}".format(self.stats['avg_100'] / self.stats['runs'],
                                                                 self.stats['avg_1000'] / self.stats['runs'],
                                                                 self.stats['bytes'])
        print ""

    def run(self):
        for _ in range(self.runs):
            self.load_random()

            if self.type == 'cache':
                self.stats['avg_100'] += self.do_random(100)
                self.stats['avg_1000'] += self.do_random(1000)
            elif self.type == 'fs':
                self.stats['avg_100'] += self.do_random(100, cache=False)
                self.stats['avg_1000'] += self.do_random(1000, cache=False)
            else:
                print "unknown test type"
                sys.exit(1)
            print "MAIN:  wait 3 seconds...\n"
            self.stats['runs'] += 1
            time.sleep(3)
        self.pstats()

if __name__ == '__main__':

    opts = salt.config.master_config('/etc/salt/master')
    args = vars(Argparser().parse_args())

    # let the things settle and the cache
    # populate before we enter the loop
    test = FSCacheTest(opts, args['runs'], args['type']).run()