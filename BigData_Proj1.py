# -*- coding: utf-8 -*-
"""
Created on Tue Oct  4 15:24:39 2022

@author: Viet Dinh
"""
from py2neo import Graph, Node, Relationship

graph = Graph()
graph.run("MATCH (a:Person) RETURN a.name, a.born LIMIT 4").data()
[{'a.born': 1964, 'a.name': 'Keanu Reeves'},
 {'a.born': 1967, 'a.name': 'Carrie-Anne Moss'},
 {'a.born': 1961, 'a.name': 'Laurence Fishburne'},
 {'a.born': 1960, 'a.name': 'Hugo Weaving'}]



