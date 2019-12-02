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


if __name__ == "__main__":
    from sys import argv

    if len(argv) != 3:
        print("Usage: %s <redis-host> <redis-passwd> " % argv[0], file=sys.stderr)
        exit(1)

    redis_port = 6379
    #        p.set(key, val, ttl) 6379
    redis_host = argv[1]
    redis_passwd = argv[2]
    arra = []
    tmp = {}
    f = open('test.txt')
    for line in f:
        dic = json.loads(line)
        print(dic)
        if dic != tmp:
            print(dic)
            arra.append(dic)
    
    rc = redis.Redis(host=redis_host, port=redis_port, password=redis_passwd)
    with rc.pipeline(transaction=False) as p:
        for res in arra:
            p.set(res["key"], res["smid"], res["ttl"]) 
        result = p.execute()
    print(result)
