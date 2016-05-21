#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2016-05-21 11:47:52
# @Author  : Damon Chen (Damon@zhenchen.me)
# @Link    : www.zhenchen.me
# @Version : $Id$
# @Description:
#
import sys
sys.path.append('..')
import networkx as nx
from Redis.RedisHelper import *
from Utils.params import *
import random
import logging

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

class Collaboration():
    """docString for Collaboration"""
    def __init__(self):
        logging.info("loading Redis data base...")
        self.mRedis = RedisHelper()
        logging.info("loading authors...")
        self.authors = self.mRedis.getAllAuthors()
        logging.info("loading authors' coauthors...")
        self.AuCoaus = self.loadAuCoauthors()
        logging.info("loading coauthor times...")
        self.CoauTimes = self.loadCoauTimes()
        logging.info("load data done!")

    def loadAuCoauthors(self):
        auCoaus = dict()
        for author in self.authors:
            auCoaus[author] = list(self.mRedis.getAuCoauthors(author))
        return auCoaus

    def loadCoauTimes(self):
        aucoauTimes = dict()
        for author in self.authors:
            for coau in self.getAuCoaus(author):
                aucoauTimes[author + ':' + coau] = self.mRedis.getAuCoauTimes(author, coau)
        return aucoauTimes

    def getCoauTimes(self, A, B):
        return self.CoauTimes.get(A + ':' + B)

    def getAuCoaus(self, A):
        return self.AuCoaus.get(A)

    def clearCache(self):
        self.authors = []
        self.auCoaus = {}
        self.CoauTimes = {}

    def isCollabLeadByCoAu(self, A, B):
        minCoauTimeAB = min(self.getCoauTimes(A, B))
        commonCoauthors = set(self.getAuCoaus(A)) & set(self.getAuCoaus(B))
        if len(commonCoauthors) < 1: return False
        for C in commonCoauthors:
            minCoauTimeAC = min(self.getCoauTimes(A, C))
            minCoauTimeBC = min(self.getCoauTimes(C, B))
            if minCoauTimeAB > max(minCoauTimeAC, minCoauTimeBC):
                return True
        return False

    def isCollabLeadNewCoAu(self, A, B):
        minCoauTimeAB = min(self.getCoauTimes(A, B))
        commonCoauthors = set(self.getAuCoaus(A)) & set(self.getAuCoaus(B))
        if len(commonCoauthors) < 1: return False
        for C in commonCoauthors:
            minCoauTimeAC = min(self.getCoauTimes(A, C))
            minCoauTimeBC = min(self.getCoauTimes(C, B))
            if minCoauTimeAC > max(minCoauTimeAB, minCoauTimeBC):
                return True
        return False

    def getCollabLeadNewCoAuProb(self):
        logging.info("getCollabLeadNewCoAuProb...")
        coauNumCLCProb = dict()
        index = 0
        for author in self.authors:
            index += 1
            if index % 1000 == 0: logging.info(index)
            coaus = self.getAuCoaus(author)
            coausNum = len(coaus)
            prob = [self.isCollabLeadNewCoAu(author, coau) for coau in coaus].count(True) * 1.0 / coausNum
            probs = coauNumCLCProb.setdefault(coausNum, [])
            probs.append(prob)
        with open(OUTPUT_COAUNUM_COLLAB_LEAD_COAU_PROB, 'w') as fileWriter:
            for coauNum, probs in coauNumCLCProb.items():
                coauNumStr = str(coauNum)
                probStr = str(sum(probs) / len(probs))
                fileWriter.write(coauNumStr + '\t' + probStr + '\n')
        fileWriter.close()
        coauNumCLCProb = {}

    def getCollabLeadNewCoaus(self):
        logging.info("getCoausLeadByCollab...")
        coauNumLeadNewCoauNums = dict()
        index = 0
        for author in self.authors:
            index += 1
            if index % 1000 == 0: logging.info(index)
            coaus = self.getAuCoaus(author)
            coausNum = len(coaus)
            newCoausNum = [self.isCollabLeadByCoAu(author, coau) for coau in coaus].count(True)
            newCoausnums = coauNumLeadNewCoauNums.setdefault(coausNum, [])
            newCoausnums.append(newCoausNum)
        with open(OUTPUT_COAUNUM_COLLAB_LEAD_NEW_COAU_NUM, 'w') as fileWriter:
            for coauNum, newCoausnums in coauNumLeadNewCoauNums.items():
                coauNumStr = str(coauNum)
                newCoausNumStr = str(sum(newCoausnums) * 1.0 / len(newCoausnums))
                fileWriter.write(coauNumStr + '\t' + newCoausNumStr + '\n')
        fileWriter.close()
        coauNumLeadNewCoauNums = {}

    def getCollabLeadPotentialCoaus(self):
        logging.info("getCollabLeadPotentialCoaus...")
        coauNumLeadPotCoaus = dict()
        index = 0
        for author in self.authors:
            index += 1
            if index % 1000 == 0: logging.info(index)
            coaus = self.getAuCoaus(author)
            coausNum = len(coaus)
            potCoausNum = sum([len(self.getAuCoaus(coau)) for coau in coaus])
            potCoausNums = coauNumLeadPotCoaus.setdefault(coausNum, [])
            potCoausNums.append(potCoausNum)
        with open(OUTPUT_COAUNUM_COLLAB_LEAD_POT_COAU_NUM, 'w') as fileWriter:
            for coauNum, potCoausNums in coauNumLeadPotCoaus.items():
                coauNumStr = str(coauNum)
                potCoausNumStr = str(sum(potCoausNums) * 1.0 / len(potCoausNums))
                fileWriter.write(coauNumStr + '\t' + potCoausNumStr + '\n')
        fileWriter.close()
        coauNumLeadPotCoaus = {}

if __name__ == '__main__':
    collaboration = Collaboration()
    # collaboration.getCollabLeadNewCoAuProb()
    # collaboration.getCollabLeadNewCoaus()
    collaboration.getCollabLeadPotentialCoaus()

    collaboration.clearCache()
