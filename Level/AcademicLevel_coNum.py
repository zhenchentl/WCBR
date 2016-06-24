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

class AcademicLevel():
    """docstring for AcademicLevel"""
    def __init__(self):
        self.mRedis = RedisHelper()
        self.authors = self.mRedis.getAllAuthors()
        self.auCoauNums =dict()
        for author in self.authors:
            self.auCoauNums[author] = len(self.mRedis.getAuCoauthors(author))
        self.coauNumCoauLevel = dict()

    def getCoauNumLevel(self):
        index = 0
        for author in self.authors:
            index += 1
            if index % 100000 == 0:
                logging.info(index)
            coaus = self.mRedis.getAuCoauthors(author)
            coauNum = len(coaus)
            coauAvgLevel = sum([float(self.auCoauNums.get(coau)) for coau in coaus]) / coauNum
            CoauLevels = self.coauNumCoauLevel.setdefault(coauNum, [])
            CoauLevels.append(coauAvgLevel)

    def saveCoauNumLevel(self):
        with open(OUTPUT_COAUNUM_LEVEL_CONUM, 'w') as fileWriter:
            for coauNum, levels in self.coauNumCoauLevel.items():
                cn = str(coauNum)
                cl = self.coauNumCoauLevel.get(coauNum)
                coAuLevel = str(sum(cl) / len(cl))
                fileWriter.write(cn + '\t' + cn + '\t' + coAuLevel + '\n')
        fileWriter.close()
        self.coauNumCoauLevel = {}

if __name__ == '__main__':
    academicLevel = AcademicLevel()
    academicLevel.getCoauNumLevel()
    academicLevel.saveCoauNumLevel()
