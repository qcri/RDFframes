from rdfframes.knowledge_graph import KnowledgeGraph
from rdfframes.client.http_client import HttpClientDataFormat, HttpClient
from rdfframes.client.sparql_endpoint_client import SPARQLEndpointClient
import time
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


def optional_expand_filter_expand():
  Films = graph.entities('dbpo:Film', entities_col_name='film')\
    .expand('film', [('dbpp:starring', 'actor'), ('dbpp:country', 'movie_country')])\
    .filter({'movie_country': [' IN (dbpr:United_States, dbpr:India)']})\
	.expand('film', [ ('dbpp:genre', 'genre')])\
	.expand('film', [ ('dbpp:director','director', True), ('dbpp:producer', 'producer', True), ('dbpp:language', 'language'), ('dbpp:story','story') ,
       ('dbpp:studio' ,'studio'), ('dbpp:title', 'title', True)])\
    .filter({'genre': ['IN (dbpr:Film_score, dbpr:Soundtrack, dbpr:Rock_music, dbpr:House_music, dbpr:Dubstep)']})\
    .filter({'studio': ['!= "Eskay Movies"']})
  print("SPARQL Query = \n{}".format(Films.to_sparql())) 
  df = Films.execute(client)
  #print(df.shape)


start_time = time.time()
optional_expand_filter_expand()
end_time = time.time()

print('Duration = {} sec'.format(end_time-start_time))


"""
Naive 
PREFIX  dbpp: <http://dbpedia.org/property/>
PREFIX  dbpo: <http://dbpedia.org/ontology/>
PREFIX  rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX  dcterms: <http://purl.org/dc/terms/>
PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  dbpr: <http://dbpedia.org/resource/>

SELECT  *
FROM <http://dbpedia.org>
WHERE
  { { SELECT  *
      WHERE
        { ?film  rdf:type  dbpo:Film }
    }
    { SELECT  *
      WHERE
        { ?film  dbpp:starring  ?actor }
    }
    { SELECT  *
      WHERE
        { ?film  dbpp:country  ?movie_country }
    }
    { SELECT  *
      WHERE
        { ?film  dbpp:genre  ?genre }
    }
    { SELECT  *
      WHERE
        { ?film  dbpp:language  ?language }
    }
    { SELECT  *
      WHERE
        { ?film  dbpp:story  ?story }
    }
    { SELECT  *
      WHERE
        { ?film  dbpp:studio  ?studio }
    }
    FILTER ( ( ( ?movie_country IN (dbpr:United_States, dbpr:India) ) && ( ?genre IN (dbpr:Film_score, dbpr:Soundtrack, dbpr:Rock_music, dbpr:House_music, dbpr:Dubstep) ) ) && ( ?studio != "Eskay Movies" ) )
    { SELECT  *
      WHERE
        { OPTIONAL
            { ?film  dbpp:director  ?director }
        }
    }
    { SELECT  *
      WHERE
        { OPTIONAL
            { ?film  dbpp:producer  ?producer }
        }
    }
    { SELECT  *
      WHERE
        { OPTIONAL
            { ?film  dbpp:title  ?title }
        }
    }
  }
"""




