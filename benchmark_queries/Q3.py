from time import time
from rdfframes.knowledge_graph import KnowledgeGraph
from rdfframes.client.http_client import HttpClientDataFormat, HttpClient
from rdfframes.client.sparql_endpoint_client import SPARQLEndpointClient
from rdfframes.utils.constants import JoinType
__author__ = "Ghadeer"


endpoint = 'http://10.161.202.101:8890/sparql/'
port = 8890
output_format = HttpClientDataFormat.PANDAS_DF
max_rows = 1000000
timeout = 12000
"""
client = HttpClient(endpoint_url=endpoint,
	port=port,
		return_format=output_format,
		timeout=timeout,
		max_rows=max_rows
		)
"""
client = SPARQLEndpointClient(endpoint)

graph =  KnowledgeGraph(graph_name='dbpedia')

def expand_groupby_join(join_type):
  basketball_palyer = graph.entities('dbpo:BasketballPlayer', entities_col_name='player')\
	.expand('player', [('dbpp:team', 'team')])\
	.group_by(['team']).count('player', 'count_basketball_players', True)
  
  basketball_team = graph.entities('dbpo:BasketballTeam', entities_col_name='team')\
  .expand('team', [('dbpp:president', 'president'), ('dbpp:sponsor', 'sponsor'), ('dbpp:name', 'name')])
  basketball_palyer_team = basketball_team.join(basketball_palyer,'team', join_type=join_type)
  #print(basketball_palyer_team.to_sparql())
  df = basketball_palyer_team.execute(client)
  print(basketball_palyer_team.to_sparql())
  df = dataset.execute(client, return_format=output_format)
  print(df.shape)
  #print(df)
  
start = time()
expand_groupby_join(JoinType.RightOuterJoin) 

duration = time()-start
print("Duration of join = {} sec".format(duration))


"""
RDFframes:


PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dbpp: <http://dbpedia.org/property/>
PREFIX dbpr: <http://dbpedia.org/resource/>
PREFIX dbpo: <http://dbpedia.org/ontology/>
SELECT * 
FROM <http://dbpedia.org>
WHERE {
        {
        SELECT DISTINCT ?team  (COUNT(DISTINCT ?player) AS ?count_basketball_players) 
        WHERE {
                ?player rdf:type dbpo:BasketballPlayer .
                ?player dbpp:team ?team .
                } GROUP BY ?team 
        }
 OPTIONAL       {
        SELECT * 
        WHERE {
                ?team rdf:type dbpo:BasketballTeam .
                ?team dbpp:president ?president .
                ?team dbpp:sponsor ?sponsor .
                ?team dbpp:name ?name .
                }

                }
        }
 """
"""
Optimized

PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dbpp: <http://dbpedia.org/property/>
PREFIX dbpr: <http://dbpedia.org/resource/>
PREFIX dbpo: <http://dbpedia.org/ontology/>
SELECT ?name ?president ?sponsor ?ount_basketball_players
FROM <http://dbpedia.org>
WHERE {
        {
        SELECT DISTINCT ?team  (COUNT(DISTINCT ?player) AS ?count_basketball_players) 
        WHERE {
                ?player rdf:type dbpo:BasketballPlayer ;
                   dbpp:team ?team .
                } GROUP BY ?team 
        }
 OPTIONAL       {
        SELECT * 
        WHERE {
                ?team rdf:type dbpo:BasketballTeam ;
                 dbpp:president ?president ;
                 dbpp:sponsor ?sponsor ;
                 dbpp:name ?name .
                }

                }
        }
 """

"""
Naive 

PREFIX  dbpp: <http://dbpedia.org/property/>
PREFIX  dcterms: <http://purl.org/dc/terms/>
PREFIX  dbpo: <http://dbpedia.org/ontology/>
PREFIX  rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  dbpr: <http://dbpedia.org/resource/>

SELECT  *
FROM <http://dbpedia.org>
WHERE
  { { SELECT  ?team ?count_basketball_players
      WHERE
        { { SELECT DISTINCT  ?team (COUNT(DISTINCT ?player) AS ?count_basketball_players)
            WHERE
              { SELECT DISTINCT  ?player ?team
                WHERE
                  { { SELECT  ?player
                      WHERE
                        { ?player  rdf:type  dbpo:BasketballPlayer }
                    }
                    { SELECT  ?player ?team
                      WHERE
                        { ?player  dbpp:team  ?team }
                    }
                  }
              }
            GROUP BY ?team
          }
        }
    }
    OPTIONAL
    {
      { SELECT  ?team
        WHERE
          { ?team  rdf:type  dbpo:BasketballTeam }
      }
    { SELECT  ?team ?president
      WHERE
        { ?team  dbpp:president  ?president }
    }
    { SELECT  ?team ?sponsor
      WHERE
        { ?team  dbpp:sponsor  ?sponsor }
    }
    { SELECT  ?team ?name
      WHERE
        { ?team  dbpp:name  ?name }
    }
    }
  }
"""
