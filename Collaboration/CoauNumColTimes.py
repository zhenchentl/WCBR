#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2016-05-20 14:59:02
# @Author  : Damon Chen (Damon@zhenchen.me)
# @Link    : www.zhenchen.me
# @Version : $Id$
# @Description:

import sys
sys.path.append('..')
import networkx as nx
from Redis.RedisHelper import *
from Utils.params import *
import random
import logging

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

class CoauNumColTimes():
    """docstring for CoauNumColTimes"""
    def __init__(self):
        self.mRedis = RedisHelper()
        self.authors = self.mRedis.getAllAuthors()
        self.CoauNumColTimes = dict()
        for author in self.authors:
            coaus = self.mRedis.getAuCoauthors(author)
            coauNum = len(coaus)
            colTime = sum([len(self.mRedis.getAuCoauTimes(author, coau)) for coau in coaus])
            colTimes = self.CoauNumColTimes.setdefault(coauNum, [])
            colTimes.append(colTime)

    def saveCoauNumColTimes(self):
        with open(OUTPUT_COAUNUM_COLTIME, 'w') as fileWriter:
            for coauNum, cts in self.CoauNumColTimes.items():
                cn = str(coauNum)
                ct = str(sum(cts) / len(cts))
                fileWriter.write(cn + '\t' + cn + '\t' + ct + '\n')
        fileWriter.close()
        self.CoauNumColTimes = {}

if __name__ == '__main__':
    # coauNumColTimes = CoauNumColTimes()
    # coauNumColTimes.saveCoauNumColTimes()
    mRedis = RedisHelper()
    print sum([len(mRedis.getAuCoauthors(au)) for au in mRedis.getAllAuthors()])
