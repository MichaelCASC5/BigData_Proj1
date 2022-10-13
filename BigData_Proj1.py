# -*- coding: utf-8 -*-
"""
Created on Tue Oct  4 15:24:39 2022

@author: Viet Dinh
@author: Michael Calle
"""
#Precursors on Neo4j db settings found in Neo4j Desktop
#Must comment out dbms.directories.import=import else can't read tsv files in other dirs
#Set dbms.memory.heap.max_size=3G (default 1G) to improve relationship creation speed


from py2neo import Graph
import typer

app = typer.Typer()
#Connects to Neo4j database
graph = Graph("bolt://localhost:11006", auth=("neo4j", "12345678"))

@app.command()
def createneodb():
    #Creates nodes with label "Node" and property "type"
    nquery = """
    LOAD CSV FROM "file:///C:/Users/merom/Desktop/Big_Data/nodes.tsv" AS line FIELDTERMINATOR "\t"
    CREATE (:Node { id: line[0], name: line[1], type: line[2]})
    """
    
    #Queries assign nodes with correct labels and removes property "type"
    nquery2 = """
    MATCH (n:Node {type:'Anatomy'})
    SET n:Anatomy
    REMOVE n:Node
    REMOVE n.type;
    """
    nquery3 = """
    MATCH (n:Node {type:'Gene'})
    SET n:Gene
    REMOVE n:Node
    REMOVE n.type;
    """
    nquery4 = """
    MATCH (n:Node {type:'Disease'})
    SET n:Disease
    REMOVE n:Node
    REMOVE n.type;
    """
    nquery5 = """ 
    MATCH (n:Node {type:'Compound'})
    SET n:Compound
    REMOVE n:Node
    REMOVE n.type;
    """
    
    #Creates relationships with metaedge as property "metaedge"
    rquery = """
    LOAD CSV WITH HEADERS FROM "file:///C:/Users/merom/Desktop/Big_Data/edges.tsv" AS row 
    FIELDTERMINATOR "\t"
    
    MATCH (n1:Node {id: row.source})
    MATCH (n2:Node {id: row.target})
    MERGE (n1)-[:Relates {metaedge: row.metaedge}]->(n2)
    """
    
    #Adding constraint MASSIVELY improves relationship creation time
    constraintQuery = """
    CREATE CONSTRAINT id ON (n:Node) ASSERT n.id IS UNIQUE
    """
    
    #Queries only need to be ran once at start to create database; comment out after
    print(graph.run(nquery).stats())
    print(graph.run(constraintQuery).stats())
    print(graph.run(rquery).stats())
    print(graph.run(nquery2).stats())
    print(graph.run(nquery3).stats())
    print(graph.run(nquery4).stats())
    print(graph.run(nquery5).stats())
    
    
@app.command()
def deleteneodb():
    print(graph.run("DROP CONSTRAINT ON (n:Node) ASSERT n.id IS UNIQUE").stats())
    print(graph.run("MATCH (n) DETACH DELETE n").stats())
    
@app.command()
def neodiseaseinfo(disease_id:str):
    #NEED TO DO
    query = """
        MATCH (d:Disease {id:$disease_id})  
        MATCH (c1:Compound)-[r1:Relates{metaedge:'CpD'}]->(d) 
        RETURN d.name AS disease,c1.name AS drug_treats,NULL AS drug_palliates, NULL AS gene_causes, NULL AS anatomy_loc
        
        UNION
        
        MATCH (d:Disease {id:$disease_id})
        MATCH (c2:Compound)-[r2:Relates{metaedge:'CtD'}]->(d)
        RETURN d.name AS disease,NULL AS drug_treats,c2.name AS drug_palliates, NULL AS gene_causes, NULL AS anatomy_loc
        
        UNION
        
        MATCH (d:Disease {id:$disease_id})
        MATCH (d)-[r3:Relates{metaedge:'DaG'}]->(g:Gene)
        RETURN d.name AS disease,NULL AS drug_treats,NULL AS drug_palliates,g.name AS gene_causes, NULL AS anatomy_loc
        
        UNION
        
        MATCH (d:Disease {id:$disease_id})
        MATCH (d)-[r4:Relates{metaedge:'DlA'}]->(a:Anatomy)
        RETURN d.name AS disease,NULL AS drug_treats,NULL AS drug_palliates,NULL AS gene_causes, a.name AS anatomy_loc
        """
    print(graph.run(query, disease_id = disease_id))

#@app.command()
#def neocmpdtreatdisease():
    #ADD QUERY
    
#REFERENCE
#@app.command()
#def hello(name:str, iq:int, display_iq:bool = True):
#    print(f"hello {name}")
#    if(display_iq):
#        print(f"your iq is: {iq}")
    
if __name__ == "__main__":
    app()

