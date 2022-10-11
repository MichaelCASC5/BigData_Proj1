# -*- coding: utf-8 -*-
"""
Created on Tue Oct  4 15:24:39 2022

@author: Viet Dinh
"""
from py2neo import Graph, Node, Relationship, cypher

graph = Graph("bolt://localhost:7687", auth=("neo4j", "12345678"))
graph.run("MATCH (n) RETURN n")