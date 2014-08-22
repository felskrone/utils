# -*- coding: utf-8 -*-
'''
This module contains routines used to verify the matcher against the minions
expected to return
'''

# Import python libs
import os
import glob
import re
import logging

# Import salt libs
import salt.payload
import salt.utils
from salt.exceptions import CommandExecutionError
from multiprocessing.pool import ThreadPool
import random
import zmq
import time

class CacheCli(object):

    def __init__(self, opts, timeout=20):
        self.opts = opts
        self.timeout = timeout
        self.setup()

    def setup(self):
        self.context = zmq.Context()
        self.cache_cli = self.context.socket(zmq.REQ)
        self.cache_cli.connect('ipc:///tmp/fsc_cache')

        self.poll = zmq.Poller()
        self.poll.register(self.cache_cli, zmq.POLLIN)

        self.serial = salt.payload.Serial(self.opts.get('serial', ''))

    def get(self, path):

        # add random id to cache-request to have a 1:1 relation
        msgid = random.randint(10000, 20000)
        msg = [msgid, path]

        try:
            # send a request to the FSCache. If we get to here after
            # the cache was too slow before, the send() will fail but we try
            # anyway going by EAFP rules.
            self.cache_cli.send(self.serial.dumps(msg))
        except zmq.error.ZMQError:
            # on failure we do a dummy recv()and resend the last msg. That
            # keeps us from having to re-init the socket on failure.
            self.cache_cli.recv()
            self.cache_cli.send(self.serial.dumps(msg))

        to_max = self.timeout*5
        to_count = 0 
        self.loop = 0

        while 1:
            self.loop += 1
            # we wait cache_timout*5
            self.socks = dict(self.poll.poll())
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
                            return reply[1]
                        else:
                            return {}
                        # received reply for request, enable the
                        # break the loop to make way for next request
                        break
                # None is a cache miss, we have to go to disk
                elif msg is None:
                    print "MAIN:  cache miss..."
                # we should never get here
                else:
                    raise zmq.error.ZMQError, "invalid state in FSCache-communication"
            break
        return {}

class CkMinions(object):
    '''
    Used to check what minions should respond from a target
    '''
    def __init__(self, opts, from_cache=False):
        self.opts = opts
        self.serial = salt.payload.Serial(opts)
        if self.opts['transport'] == 'zeromq':
            self.acc = 'minions'
        else:
            self.acc = 'accepted'

        if from_cache:
            self.cache = CacheCli(self.opts)
        else:
            self.cache = None

    def connected_ids(self, subset=None, show_ipv4=False, **kwargs):
        '''
        Return a set of all connected minion ids, optionally within a subset
        '''
        minions = set()
        if self.opts.get('minion_data_cache', False):
            cdir = os.path.join(self.opts['cachedir'], 'minions')
            if not os.path.isdir(cdir):
                return minions
            addrs = salt.utils.network.local_port_tcp(int(self.opts['publish_port']))
            if subset:
                search = subset
            else:
                search = os.listdir(cdir)
            for id_ in search:
                datap = os.path.join(cdir, id_, 'data.p')

                if self.cache:
                    try:
                        grains = self.serial.loads(
                            self.cache.get(datap)
                        ).get('grains', {})
                    except AttributeError:
                        pass

                else:
                    if not os.path.isfile(datap):
                        continue
                    try:
                        grains = self.serial.load(
                            salt.utils.fopen(datap, 'rb')
                        ).get('grains', {})
                    except AttributeError:
                        pass
                for ipv4 in grains.get('ipv4', []):
                    if ipv4 == '127.0.0.1' or ipv4 == '0.0.0.0':
                        continue
                    if ipv4 in addrs:
                        if show_ipv4:
                            minions.add((id_, ipv4))
                        else:
                            minions.add(id_)
                        break
        return minions

if __name__ == '__main__':
    opts =salt.config.master_config('/etc/salt/master')
    from_cache = False
    minions = CkMinions(opts, from_cache)

    t_start = time.time()
    print "minions: {0}".format(len(minions.connected_ids()))
    print "time: {0}".format(time.time() - t_start)

