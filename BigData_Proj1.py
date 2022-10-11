# -*- coding: utf-8 -*-
"""
Created on Tue Oct  4 15:24:39 2022

@author: Viet Dinh
"""
from py2neo import Graph, Node, Relationship, cypher

graph = Graph("bolt://localhost:7687", auth=("neo4j", "12345678"))
graph.run("LOAD CSV FROM ‘C:\\Users\merom\Desktop\Big Data\nodes.tsv' AS line \
          FIELDTERMINATOR ‘\t’ CREATE (:Node { id: line[1], name: line[2], type: line[3]})")

