#!/usr/bin/python

import pwd
import os
import re
import sys
import pprint
import time
import socket
import argparse

PROC_TCP = "/proc/net/tcp"
STATE = {
        '01':'ESTABLISHED',
        '02':'SYN_SENT',
        '03':'SYN_RECV',
        '04':'FIN_WAIT1',
        '05':'FIN_WAIT2',
        '06':'TIME_WAIT',
        '07':'CLOSE',
        '08':'CLOSE_WAIT',
        '09':'LAST_ACK',
        '0A':'LISTEN',
        '0B':'CLOSING'
        }

class ArgParser(object):

    def __init__(self):
        self.main_parser = argparse.ArgumentParser()
        self.addArgs()

    def addArgs(self):

        self.main_parser.add_argument('-t',
                                      type=int,
                                      default=2,
                                      dest='delay',
                                      required=False,
                                      help='the delay after each run')

        self.main_parser.add_argument('-r',
                                      type=int,
                                      default=0,
                                      dest='runs',
                                      required=False,
                                      help='the number of runs to execute')

    def parseArgs(self):
        return self.main_parser.parse_args()



def _load():
    '''
    Read the table of tcp connections & remove header
    '''
    with open(PROC_TCP,'r') as f:
        content = f.readlines()
        content.pop(0)
    return content

def _hex2dec(s):
    return str(int(s,16))

def _ip(s):
    ip = [(_hex2dec(s[6:8])),(_hex2dec(s[4:6])),(_hex2dec(s[2:4])),(_hex2dec(s[0:2]))]
    return '.'.join(ip)

def _remove_empty(array):
    return [x for x in array if x !='']

def _convert_ip_port(array):
    host,port = array.split(':')
    return _ip(host),_hex2dec(port)

def _print_head():
    print "Connected/4505\tPushes/4506\tEstablished/Other"

def netstat(delay=5, runs=0):
    result = {
              'connected/4505' : 0,
              'pushes/4506' : 0,
              'pushes/estab' : 0,
              'pushes/other' : 0
             }

    run_count = 1
    _print_head()

    while True:
        content = _load()
        for line in content:
            line_array = _remove_empty(line.split(' '))
            l_host, l_port = _convert_ip_port(line_array[1])
            r_host, r_port = _convert_ip_port(line_array[2]) 
            state = STATE[line_array[3]]

            if l_port == '4505':
                if r_host != '0.0.0.0':
                    result['connected/4505'] += 1

            elif l_port == '4506':
                if r_host != '0.0.0.0':
                    result['pushes/4506'] += 1
                    if state == 'ESTABLISHED':
                        result['pushes/estab'] += 1
                    else:
                        result['pushes/other'] += 1

        print "{0}\t\t{1}/s\t\t{2}/{3}".format(result['connected/4505'],
                                             result['pushes/4506'],
                                             result['pushes/estab'],
                                             result['pushes/other'])


        if runs != 0:
            if run_count >= runs:
                sys.exit(1)

        if (run_count % 10) == 0:
                _print_head()

        time.sleep(delay)

        result['connected/4505'] = 0
        result['pushes/4506'] = 0
        result['pushes/estab'] = 0
        result['pushes/other'] = 0
        run_count += 1

if __name__ == '__main__':

    args = vars(ArgParser().parseArgs())
    try:
        netstat(delay=args['delay'], 
                runs=args['runs'])
    except KeyboardInterrupt:
        sys.exit(1)


