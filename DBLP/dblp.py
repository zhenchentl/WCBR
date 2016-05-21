#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2016-05-05 18:57:18
# @Author  : Damon Chen (Damon@zhenchen.me)
# @Link    : www.zhenchen.me
# @Version : $Id$
# @Description: dblp

import os

import sys
sys.path.append('..')
from Redis.RedisHelper import RedisHelper
from Utils.params import *
from xml.sax import handler, make_parser
import logging

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

paperLabels = ('article','inproceedings','proceedings','book', 'incollection','phdthesis', \
    'mastersthesis','www')

class dblpHandler(handler.ContentHandler):
    """docstring for dblpHandler"""
    def __init__(self):
        self.mRedis = RedisHelper()
        self.isPaperTag = False
        self.isAuthorTag = False
        self.isYearTag = False
        self.authors = list()
        self.year = ''
        self.count = 0

    def startDocument(self):
        logging.info('Document start...')

    def endDocument(self):
        logging.info('Document End...')

    def startElement(self, name, attrs):
        if name in paperLabels:
            self.isPaperTag = True
        if self.isPaperTag:
            if name == 'author':
                self.isAuthorTag = True
            if name == 'year':
                self.isYearTag = True

    def endElement(self, name):
        if name in paperLabels:
            if self.isPaperTag:
                self.count += 1
                self.isPaperTag = False
                self.mRedis.addPaperItem(self.authors, self.year)
                if self.count % 1000 == 0:
                    logging.info(self.count)
                self.authors = []
                self.year = ''

    def characters(self, content):
        if self.isYearTag:
            self.year = content
            self.isYearTag = False
        if self.isAuthorTag:
            self.authors.append(content)
            self.isAuthorTag = False

def parserDblpXml():
    handler = dblpHandler()
    parser = make_parser()
    parser.setContentHandler(handler)
    f = open(DBLP_XML_PATH,'r')
    parser.parse(f)
    f.close()

if __name__ == '__main__':
    parserDblpXml()
