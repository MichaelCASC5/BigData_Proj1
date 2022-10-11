# -*- coding: utf-8 -*-
"""
Created on Tue Oct  4 15:24:39 2022

@author: Viet Dinh
"""
from py2neo import Graph, Node, Relationship, cypher

#creates all nodes with unique label, need to edit to have nodes with correct label
graph = Graph("bolt://localhost:11003", auth=("neo4j", "12345678"))
query = """
LOAD CSV FROM "file:///C:/Users/merom/Desktop/Big_Data/nodes.tsv" AS line FIELDTERMINATOR "\t"
CREATE (:Node { id: line[0], name: line[1], type: line[2]})
"""

print(graph.run(query).stats())
