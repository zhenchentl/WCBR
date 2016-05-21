#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2016-05-05 15:29:04
# @Author  : Damon Chen (Damon@zhenchen.me)
# @Link    : www.zhenchen.me
# @Version : $Id$
# @Description: Redis util file

import os
import sys
import redis
import logging

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

IP = '127.0.0.1'
PORT = 6379

DB_AU_COAU_SET = 0
DB_AU_COAU_TIME_SET = 1
DB_AU_PR_VALUE = 2

class RedisHelper():
    """docstring for RedisHelper"""
    def __init__(self):
        try:
            self.AuCoauSet = redis.StrictRedis(IP, PORT, db = DB_AU_COAU_SET)
            self.AuCoauTimeSet = redis.StrictRedis(IP, PORT, db = DB_AU_COAU_TIME_SET)
            self.AuPRSet = redis.StrictRedis(IP, PORT, db = DB_AU_PR_VALUE)
        except:
            logging.error("can not open redis database !")

    def addPaperItem(self, authors, year):
        "增加一条论文的信息到Redis数据库中"
        try:
            time = int(year)
            for au in authors:
                for coau in authors:
                    if au != coau:
                        self.addAuCoauthor(au, coau, time)
        except:
            logging.error("year format error:" + year + "!")

    def addAuCoauthor(self, author, coauthor, time):
        self.AuCoauSet.sadd(author, coauthor)
        self.AuCoauTimeSet.sadd(author + ":" + coauthor, time)

    def addAuthorPR(self, author, PR):
        self.AuPRSet.set(author, PR)

    def getAllAuthors(self):
        return self.AuCoauSet.keys()

    def getAuCoauthors(self, author):
        return self.AuCoauSet.smembers(author)

    def getAuCoauTimes(self, author, coau):
        return self.AuCoauTimeSet.smembers(author + ":" + coau)

    def getAuthorPR(self, author):
        return self.AuPRSet.get(author)

    def getAucoauTimeKeys(self):
        return self.AuCoauTimeSet.keys()
