#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from multiprocessing import Pool
from time import time
#from sekeeper import Sekeeper
import sys
import json
import redis

#_sekeeper = Sekeeper(host="10.141.88.134")
#_sekeeper.Prepare("phone")
_rc = None

def init_redis(host, port, passwd):
    global _rc
    _rc = redis.Redis(host=host, port=port, password=passwd)

def match_and_format(key):
    dict = {}
    try:
        smid = _rc.get(key)
    except Exception as e:
        return dict
    dict["key"] = key
    dict["value"] = smid
    tl = rc.ttl(key)
    dict["ttl"] = tl
    return dict 

if __name__ == "__main__":
    from sys import argv

    if len(argv) != 3:
        print("Usage: %s <redis-host> <redis-passwd> " % argv[0], file=sys.stderr)
        exit(1)

    redis_port = 6379
    redis_host = argv[1]
    redis_passwd = argv[2]
    
    rc = redis.Redis(host=redis_host, port=redis_port, password=redis_passwd)
    key_it = rc.scan_iter("fp:*", count = 4000)
    pool = Pool(processes=1, initializer=init_redis, initargs=(redis_host, redis_port, redis_passwd))
    result_it = pool.imap_unordered(match_and_format, key_it, 1000)
    target = {}    
    total = 0
    with open("test.txt","a+") as file:
        for res in result_it:
            try:
                total += 1
                if total % 10000 == 0:
                    pass
                smid = None
                if None != res:
                    print(json.dumps(res))
                    file.write(json.dumps(res) + '\n')
            except Exception as e:
                pass
