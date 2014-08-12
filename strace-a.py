#!/usr/bin/python

import os
import re
import sys
import time
import argparse

class ArgParser(object):

    def __init__(self):
        self.main_parser = argparse.ArgumentParser()
        self.addArgs()

    def addArgs(self):

        self.main_parser.add_argument('-f',
                                      type=str,
                                      default=None,
                                      dest='filename',
                                      required=True,
                                      help='the file to analyze')

    def parseArgs(self):
        return self.main_parser.parse_args()

stats = {}

files = {}

def dofile(fname=None):

    f = open(fname, 'r')
    lines = 0
    f_data_p = 0
    f_other = 0
    for line in f:
        li = line.split()

        cmd = li[2].split('(')[0]

        if cmd not in stats.keys():
            stats[cmd] = 0
        stats[cmd] += 1   
        lines += 1

        if cmd == 'open':
            ofname = line.split('"')[1]
            if ofname not in files:
                files[ofname] = 0 

            files[ofname] += 1
            if ofname.endswith('data.p'):
                f_data_p += 1
            else:
                f_other += 1

    sorted_li = sorted(stats.items(), key=lambda x: x[1])

    print "FILE: {0} / {1} LINES".format(fname, lines)
    print "SYSCALLS:"
    for item in reversed(sorted_li):
        print item

    print "\nFILETYPES:"
    print "data.p's: {0}".format(f_data_p)
    print "others: {0}".format(f_other)

    print "\nUNIQUE FILES:"
    print len(files)

if __name__ == '__main__':

    args = vars(ArgParser().parseArgs())
    try:
        dofile(fname=args['filename'])
    except KeyboardInterrupt:
        sys.exit(1)


