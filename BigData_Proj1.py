# -*- coding: utf-8 -*-
"""
Created on Tue Oct  4 15:24:39 2022

@author: Viet Dinh
"""
from py2neo import Graph, Node, Relationship, cypher

#Nodes have labels of [Disease, Gene, Compound, Anatomy]
#Nodes have columns of id Ex:Compound::DB00661 and name Ex:Verapamil

graph = Graph("bolt://localhost:11003", auth=("neo4j", "12345678"))

query = """
LOAD CSV FROM "file:///C:/Users/merom/Desktop/Big_Data/nodes.tsv" AS line FIELDTERMINATOR "\t"
CREATE (:Node { id: line[0], name: line[1], type: line[2]})
"""
#These queries assign nodes with correct labels
query2 = """
MATCH (n:Node {type:'Anatomy'})
SET n:Anatomy
REMOVE n:Node
REMOVE n.type;
"""

query3 = """
MATCH (n:Node {type:'Gene'})
SET n:Gene
REMOVE n:Node
REMOVE n.type;
"""

query4 = """
MATCH (n:Node {type:'Disease'})
SET n:Disease
REMOVE n:Node
REMOVE n.type;
"""

query5 = """ 
MATCH (n:Node {type:'Compound'})
SET n:Compound
REMOVE n:Node
REMOVE n.type;
"""

print(graph.run(query).stats())
print(graph.run(query2).stats())
print(graph.run(query3).stats())
print(graph.run(query4).stats())
print(graph.run(query5).stats())

