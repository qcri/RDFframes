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


def expand_groupby_expand():
  basketball_palyer = graph.entities('dbpo:Athlete', entities_col_name='player')\
  .expand('player', [('dbpp:team', 'team')])\
  .group_by(['team']).count('player', 'count_basketball_players', True)\
  .expand('team', [('dbpp:name', 'name')])
  #print("SPARQL QUERY \n{}\n".format(basketball_palyer.to_sparql()))
  df = basketball_palyer.execute(client)
  #print(df.shape)


#start = time()
#expand_groupby_expand() ## change the type here.
#duration = time()-start
#print("Duration of expandable grouped expanded datasets = {} sec".format(duration))
"""
Naive query
PREFIX  dbpp: <http://dbpedia.org/property/>
PREFIX  dbpo: <http://dbpedia.org/ontology/>
PREFIX  rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX  dcterms: <http://purl.org/dc/terms/>
PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  dbpr: <http://dbpedia.org/resource/>

SELECT  *
FROM <http://dbpedia.org>
WHERE
  { { SELECT  ?team
      WHERE
        { ?team  dbpp:name  ?name }
    }
    { SELECT DISTINCT  ?team (COUNT(DISTINCT ?player) AS ?count_basketball_players)
      WHERE
        { { SELECT  *
            WHERE
              { ?player  rdf:type  dbpo:Athlete }
          }
          { SELECT  *
            WHERE
              { ?player  dbpp:team  ?team }
          }
        }
      GROUP BY ?team
    }
    { SELECT  ?team
      WHERE
        { { SELECT  ?team
            WHERE
              { ?team  dbpp:name  ?name }
          }
          { SELECT DISTINCT  ?team (COUNT(DISTINCT ?player) AS ?count_basketball_players)
            WHERE
              { { SELECT  *
                  WHERE
                    { ?player  rdf:type  dbpo:Athlete }
                }
                { SELECT  *
                  WHERE
                    { ?player  dbpp:team  ?team }
                }
              }
            GROUP BY ?team
          }
        }
    }
  }
"""


def expand_groupby_multiple():
    Films = graph.entities('dbpo:Film', entities_col_name='film')\
    .expand('film', [('dbpp:starring', 'actor'), ('dbpp:director','director', True), ('dbpp:country', 'movie_country'), 
      ('dbpp:producer', 'producer', True), ('dbpp:language', 'language'), ('dbpp:story','story') ,
      ('dbpp:runtime', 'runtime'), ('dbpp:studio' ,'studio', True), ('dbpp:title', 'title'),
      ('dbpp:genre', 'genre')])\
      .group_by(['movie_country','genre']).count('film', 'film_country_genre', True)
    df = Films.execute(client)
    #print(Films.to_sparql())
    #print(df.shape)

start = time()
expand_groupby_multiple()
duration = time()-start
print("Duration of groupby 2 columns = {} sec".format(duration))

