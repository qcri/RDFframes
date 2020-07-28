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
graph1 =  KnowledgeGraph(graph_name='dbpedia')
graph2 = KnowledgeGraph(graph_name='yago',
	graph_uri='http://yago-knowledge.org/',
	prefixes={
		'rdfs': 'http://www.w3.org/2000/01/rdf-schema#', 
		'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
		'yago': 'http://yago-knowledge.org/resource/',
		'yagoinfo': 'http://yago-knowledge.org/resource/infobox/en/'

	})

def join(join_type):
	dbpedia_actors = graph1.feature_domain_range('dbpp:starring', 'film', 'actor')\
        .expand('actor', [('dbpp:birthPlace', 'actor_country')])\
        .filter({'actor_country': ['regex(str(?actor_country), "USA")']})
	
	yago_actors = graph2.feature_domain_range('yago:actedIn', 'actor', 'film')\
	    .expand('actor', [('yago:isCitizenOf', 'actor_country')])\
		.filter({'actor_country': ['= yago:United_States']})

	actors = dbpedia_actors.join(yago_actors, 'actor', join_type=join_type)
	print(actors.to_sparql())
	#print("SPARQL Query = \n{}".format(dataset.to_sparql()))

	df = actors.execute(client, return_format=output_format)
	print(df.shape)

#(3008, 3)

start = time()
join(JoinType.OuterJoin) ## change the type here.
duration = time()-start
print("Duration of outer join  = {} sec".format(duration))

"""
RDFFrames 
PREFIX  dbpp: <http://dbpedia.org/property/>
PREFIX  dbpo: <http://dbpedia.org/ontology/>
PREFIX  rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX  yagoinfo: <http://yago-knowledge.org/resource/infobox/en/>
PREFIX  yago: <http://yago-knowledge.org/resource/>
PREFIX  dcterms: <http://purl.org/dc/terms/>
PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  dbpr: <http://dbpedia.org/resource/>

SELECT  *
WHERE
  { GRAPH <http://dbpedia.org>
      { ?film   dbpp:starring    ?actor .
        ?actor  dbpp:birthPlace  ?actor_country
        FILTER regex(str(?actor_country), "USA")
      }
    GRAPH <http://yago-knowledge.org/>
      { ?actor  yago:actedIn      ?film ;
                yago:isCitizenOf  ?actor_country
        FILTER ( ?actor_country = yago:United_States )
      }
  }
"""
PREFIX  dbpp: <http://dbpedia.org/property/>
PREFIX  dbpo: <http://dbpedia.org/ontology/>
PREFIX  rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX  yago: <http://yago-knowledge.org/resource/>
PREFIX  dcterms: <http://purl.org/dc/terms/>
PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  dbpr: <http://dbpedia.org/resource/>
PREFIX yagoinfo: <http://yago-knowledge.org/resource/infobox/en/>

SELECT  count( distinct ?name)
WHERE
  {   { SELECT  ?name
        WHERE
          { GRAPH <http://dbpedia.org>
          {?film   dbpp:starring ?actor .
            ?actor  dbpp:birthPlace  ?actor_country .
            ?actor dbpp:name ?name .
            
          }
          FILTER (?actor_country = dbpr:United_States)
          }
      }
      { SELECT ?name
        WHERE
          { GRAPH <http://yago-knowledge.org/>
              { ?actor  yago:actedIn      ?film ;
                        yago:isCitizenOf  ?actor_country;
                        yagoinfo:name ?name .
              }
            FILTER (?actor_country = yago:United_States)
          }
      }
  }








