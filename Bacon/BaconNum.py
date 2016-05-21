#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2016-05-09 16:32:46
# @Author  : Damon Chen (Damon@zhenchen.me)
# @Link    : www.zhenchen.me
# @Version : $Id$
# @Description:

import os


import sys
sys.path.append('..')
import networkx as nx
from Redis.RedisHelper import *
from Utils.params import *
import random
import logging

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

class BaconNumber(object):
    """docstring for BaconNumber"""
    def __init__(self):
        self.G = nx.Graph()
        self.stars = dict()
        self.targets = dict()
        self.loadStarsAndTargets()
        logging.info('loadStarsAndTargets done---------------')
        self.shortestPathLength = dict()
        self.mRedis = RedisHelper()
        self.buildGraph()
        logging.info('---------------')

    def buildGraph(self):
        authors = self.mRedis.getAllAuthors()
        index = 0
        for author in authors:
            index += 1
            if index % 1000 == 0:
                logging.info(index)
            coaus = self.mRedis.getAuCoauthors(author)
            for coau in coaus:
                self.G.add_edge(author, coau)

    def getGraphNodeCount(self):
        return len(self.G.nodes())

    def getGraphEdgeCount(self):
        return len(self.G.edges())

    def shortestPath(self, s, t):
        return nx.shortest_path_length(self.G, s, t)

    def getShortestPathLength(self):
        self.targets = dict(sorted(self.targets.iteritems(), key = lambda d:d[1]))
        index = 0
        for author, coausNum in self.targets.items():
            for star in self.stars.keys():
                try:
                    length = self.shortestPath(author, star)
                except:
                    length = -1
                tmp = self.shortestPathLength.setdefault(author, [])
                tmp.append(length)
            index += 1
            logging.info(str(index))
        with open(OUTPUT_AUTHORS_BACON_NUM, 'w') as fileWriter:
            nodeCount = self.getGraphNodeCount()
            edgesCount = self.getGraphEdgeCount()
            fileWriter.write('nodes:' + str(nodeCount) + '\t' + 'edges:' + str(edgesCount) + '\n')
            logging.info('nodes:' + str(nodeCount) + '\t' + 'edges:' + str(edgesCount) + '\n')
            for author, bacons in self.shortestPathLength.items():
                baconStr = ''
                count, sumB, avg = 0, 0.0, 0.0
                for bacon in bacons:
                    baconStr += str(bacon) + '\t'
                    if bacon > 0 and bacon < 10000:
                        sumB += bacon
                        count += 1
                avg = 0 if count == 0 else sumB * 1.0 / count
                sb = author + '\t' + str(self.targets[author].strip('\n')) + '\t' + str(avg) + '\t' + baconStr + '\n'
                fileWriter.write(sb)
        fileWriter.close()
        self.shortestPathLength = {}
        self.G = None

    def loadStarsAndTargets(self):
        with open(OUTPUT_STAR_AUTHORS) as fileReader:
            for line in fileReader:
                star = line.split('\t')[0]
                coauNUm = line.split('\t')[1]
                self.stars[star] = coauNUm
        fileReader.close()
        with open(OUTPUT_TARGET_AUTHORS) as fileReader:
            for line in fileReader:
                target = line.split('\t')[0]
                coauNUm = line.split('\t')[1]
                self.targets[target] = coauNUm
        fileReader.close()

def extracStarsAndTargets():
    mRedis = RedisHelper()
    stars = dict()
    targets = dict()
    authors = mRedis.getAllAuthors()
    CoAuthorNumbers = dict()
    AuthorPRs = dict()
    index = 0
    for author in authors:
        index += 1
        if index % 1000 == 0:
            logging.info(index)
        coausNum = len(mRedis.getAuCoauthors(author))
        tmp = CoAuthorNumbers.setdefault(coausNum, [])
        tmp.append(author)
        AuthorPRs[author] = mRedis.getAuthorPR(author)
    logging.info('Extracting target authors ...')
    for i in range(1, 251):
        logging.info(i)
        coaus = CoAuthorNumbers[i]
        if len(coaus) <= 100:
            for au in coaus:
                targets[au] = i
        for j in range(100):
            au = random.choice(coaus)
            if au not in targets.keys():
                targets[au] = i
    candidateStars = sorted(AuthorPRs.iteritems(), key = lambda d:d[1], reverse = True)[0:400]
    count = 0
    while count < 100:
        star, PR = random.choice(candidateStars)
        if star not in stars:
            stars[star] = PR
            count += 1
            logging.info(count)
    authors = []
    CoAuthorNumbers = {}
    AuthorPRs = {}
    candidateStars = {}

    with open(OUTPUT_STAR_AUTHORS, 'w') as fileWriter:
        for star, PR in stars.items():
            fileWriter.write(star + '\t' + str(PR) + '\n')
    fileWriter.close()
    with open(OUTPUT_TARGET_AUTHORS, 'w') as fileWriter:
        for author, CoauNum in targets.items():
            fileWriter.write(author + '\t' + str(CoauNum) + '\n')
    fileWriter.close()

if __name__ == '__main__':
    # extracStarsAndTargets()

    # baconNum = BaconNumber()
    # baconNum.getShortestPathLength()

    CoauNumBCNDict = dict()
    with open(OUTPUT_AUTHORS_BACON_NUM) as fileReader:
        for line in fileReader:
            coauNum = int(line.split('\t')[1])
            avgBCN = float(line.split('\t')[2])
            if avgBCN < 0.5:
                continue
            tmp = CoauNumBCNDict.setdefault(coauNum, [])
            tmp.append(avgBCN)
    fileReader.close()
    for k, v in CoauNumBCNDict.items():
        avg = sum(v) * 1.0 / len(v)
        maxB = max(v)
        minB = min(v)
        print str(k) + '\t' + str(avg) + '\t' + str(maxB) + '\t' + str(minB)
