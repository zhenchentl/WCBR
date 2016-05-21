#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2016-05-08 11:13:16
# @Author  : Damon Chen (Damon@zhenchen.me)
# @Link    : www.zhenchen.me
# @Version : $Id$
# @Description:

import os
import sys
sys.path.append('..')
from Utils.params import *
import networkx as nx
from Redis.RedisHelper import *
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', \
    level=logging.INFO)

class Graph():
    """docstring for Graph"""
    def __init__(self):
        self.graph = nx.DiGraph()

    def getDiGraph(self):
        mRedis = RedisHelper()
        authors = mRedis.getAllAuthors()
        count = 0
        for author in authors:
            count += 1
            if count % 1000 == 0:
                logging.info(count)
            for coau in mRedis.getAuCoauthors(author):
                self.graph.add_edge(author, coau)
        logging.info('load graph done!')
        logging.info('nodes:' + str(self.graph.number_of_nodes()))
        logging.info('edges:' + str(self.graph.number_of_edges()))
        return self.graph

# class RandomWalk():
#     def __init__(self):
#         self.S = dict()

#     def PageRank(self, graph, damping_factor = 0.8, max_iterations = 100, \
#         min_deta = 0.000001):
#         nodes = graph.nodes()
#         graph_size = graph.number_of_nodes()
#         pagerank = dict.fromkeys(nodes, 1.0 / graph_size)
#         min_value = (1 - damping_factor) * 1.0 / graph_size
#         for i in range(max_iterations):
#             diff = 0
#             for node in nodes:
#                 rank = min_value
#                 for referring_node in graph.neighbors(node):
#                     rank += damping_factor * pagerank[referring_node] / graph.degree(referring_node)
#                 diff += abs(pagerank[node] - rank)
#                 pagerank[node] = rank
#             if diff < min_deta:
#                 break
#         logging.info('itertimes:' + str(i))
#         return pagerank



"""PageRank analysis of graph structure. """
#    BSD license.
#    NetworkX:http://networkx.lanl.gov/
def pagerank(G, alpha=0.85, personalization=None,
             max_iter=100, tol=1.0e-9, nstart=None, weight='weight',
             dangling=None):
    """Return the PageRank of the nodes in the graph.
    Parameters
    -----------
    G : graph
        A NetworkX graph. 在PageRank算法里面是有向图
    alpha : float, optional
        稳定系数, 默认0.85, 心灵漂移teleporting系数，用于解决spider trap问题
    personalization: dict, optional
      个性化向量，确定在分配中各个节点的权重
      格式举例，比如四个点的情况: {1:0.25,2:0.25,3:0.25,4:0.25}
      默认个点权重相等，也可以给某个节点多分配些权重，需保证权重和为1.
    max_iter : integer, optional
        最大迭代次数
    tol : float, optional
        迭代阈值
    nstart : dictionary, optional
        整个网络各节点PageRank初始值
    weight : key, optional
      各边权重

    dangling: dict, optional
      字典存储的是dangling边的信息
      key   --dangling边的尾节点，也就是dangling node节点
      value --dangling边的权重
      PR值按多大程度将资源分配给dangling node是根据personalization向量分配的
      This must be selected to result in an irreducible transition
      matrix (see notes under google_matrix). It may be common to have the
      dangling dict to be the same as the personalization dict.

    Notes
    -----
    特征值计算是通过迭代方法进行的，不能保证收敛，当超过最大迭代次数时，还不能减小到阈值内，就会报错

    """

    #步骤一：图结构的准备--------------------------------------------------------------------------------
    if len(G) == 0:
        return {}

    if not G.is_directed():
        D = G.to_directed()
    else:
        D = G

    # Create a copy in (right) stochastic form
    W = nx.stochastic_graph(D, weight=weight)
    N = W.number_of_nodes()


    # 确定PR向量的初值
    if nstart is None:
        x = dict.fromkeys(W, 1.0 / N)  #和为1
    else:
        # Normalized nstart vector
        s = float(sum(nstart.values()))
        x = dict((k, v / s) for k, v in nstart.items())

    if personalization is None:
        # Assign uniform personalization vector if not given
        p = dict.fromkeys(W, 1.0 / N)
    else:
        missing = set(G) - set(personalization)
        if missing:
            raise NetworkXError('Personalization dictionary '
                                'must have a value for every node. '
                                'Missing nodes %s' % missing)
        s = float(sum(personalization.values()))
        p = dict((k, v / s) for k, v in personalization.items()) #归一化处理

    if dangling is None:
        # Use personalization vector if dangling vector not specified
        dangling_weights = p
    else:
        missing = set(G) - set(dangling)
        if missing:
            raise NetworkXError('Dangling node dictionary '
                                'must have a value for every node. '
                                'Missing nodes %s' % missing)
        s = float(sum(dangling.values()))
        dangling_weights = dict((k, v/s) for k, v in dangling.items())

    dangling_nodes = [n for n in W if W.out_degree(n, weight=weight) == 0.0]

    #dangling_nodes  dangling节点
    #danglesum       dangling节点PR总值

    #dangling初始化  默认为personalization
    #dangling_weights  根据dangling而生成，决定dangling node资源如何分配给全局的矩阵


    # 迭代计算--------------------------------------------------------------------

    #PR=alpha*(A*PR+dangling分配)+(1-alpha)*平均分配
    #也就是三部分，A*PR其实是我们用图矩阵分配的，dangling分配则是对dangling node的PR值进行分配，(1-alpha)分配则是天下为公大家一人一份分配的

    #其实通俗的来说，我们可以将PageRank看成抢夺大赛，有三种抢夺机制。
    #1，A*PR这种是自由分配，大家都愿意参与竞争交流的分配
    #2，dangling是强制分配，有点类似打倒土豪分田地的感觉，你不参与自由市场，那好，我们就特地帮你强制分。
    #3，平均分配，其实就是有个机会大家实现共产主义了，不让spider trap这种产生rank sink的节点捞太多油水，其实客观上也是在帮dangling分配。

    #从图和矩阵的角度来说，可以这样理解，我们这个矩阵可以看出是个有向图
    #矩阵要收敛-->矩阵有唯一解-->n阶方阵对应有向图是强连通的-->两个节点相互可达，1能到2,2能到1
    #如果是个强连通图，就是我们上面说的第1种情况，自由竞争，那么我们可以确定是收敛的
    #不然就会有spider trap造成rank sink问题


    for _ in range(max_iter):
        print 'itertime:', _
        xlast = x
        x = dict.fromkeys(xlast.keys(), 0)  #x初值
        danglesum = alpha * sum(xlast[n] for n in dangling_nodes) #第2部分：计算dangling_nodes的PR总值
        for n in x:
            for nbr in W[n]:
                x[nbr] += alpha * xlast[n] * W[n][nbr][weight]    #第1部分:将节点n的PR资源分配给各个节点，循环之
        for n in x:
            x[n] += danglesum * dangling_weights[n] + (1.0 - alpha) * p[n]   #第3部分：节点n加上dangling nodes和均分的值

        # 迭代检查
        err = sum([abs(x[n] - xlast[n]) for n in x])
        if err < N*tol:
            return x
    return x
    raise NetworkXError('pagerank: power iteration failed to converge '
                        'in %d iterations.' % max_iter)

if __name__ == '__main__':
    mRedis = RedisHelper()
    graph = Graph()
    G = graph.getDiGraph()
    pagerank = pagerank(G, max_iter = 30, tol = 0)
    logging.info('pagerank lentgh:' + str(len(pagerank)))
    count = 0
    for k, v in pagerank.items():
        count += 1
        if count % 1000 == 0:
            logging.info(count)
        mRedis.addAuthorPR(k, v)
    graph = None
    pagerank = None

