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

client = HttpClient(endpoint_url=endpoint,
                    port=port,
                    return_format=output_format,
                    timeout=timeout,
                    max_rows=max_rows
                    )

client = SPARQLEndpointClient(endpoint)
graph1 = KnowledgeGraph(graph_name='dbpedia')
graph2 = KnowledgeGraph(graph_name='yago',
                        graph_uri='http://yago-knowledge.org/',
                        prefixes={
                            'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
                            'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
                            'yago': 'http://yago-knowledge.org/resource/',
                            'yagoinfo': 'http://yago-knowledge.org/resource/infobox/en/'

                        })


def join_warning(join_type):
    dbpedia_actors = graph1.feature_domain_range('dbpp:starring', 'film', 'actor') \
        .expand('actor', [('dbpp:birthPlace', 'actor_country'), ('dbpp:name', 'name')]) \
        .filter({'actor_country': ['regex(str(?actor_country), "USA")']})#.select_cols(['name'])

    yago_actors = graph2.feature_domain_range('yago:actedIn', 'actor', 'film') \
        .expand('actor', [('yago:isCitizenOf', 'actor_country'), ('yagoinfo:name', 'name')]) \
        .filter({'actor_country': ['= yago:United_States']})#.select_cols(['name'])

    actors = dbpedia_actors.join(yago_actors, 'name', join_type=join_type)
    print(actors.to_sparql())


def join(join_type):
    dbpedia_actors = graph1.feature_domain_range('dbpp:starring', 'film1', 'actor1') \
        .expand('actor1', [('dbpp:birthPlace', 'actor_country1'), ('dbpp:name', 'name')]) \
        .filter({'actor_country1': ['regex(str(?actor_country1), "USA")']})#.select_cols(['name'])

    yago_actors = graph2.feature_domain_range('yago:actedIn', 'actor2', 'film2') \
        .expand('actor2', [('yago:isCitizenOf', 'actor_country2'), ('yagoinfo:name', 'name')]) \
        .filter({'actor_country2': ['= yago:United_States']})#.select_cols(['name'])

    actors = dbpedia_actors.join(yago_actors, 'name', join_type=join_type)
    print(actors.to_sparql())

    #df = actors.execute(client, return_format=output_format)
    #print(df.shape)


def join_grouped_expandable(join_type):
    dbpedia_actors = graph1.feature_domain_range('dbpp:starring', 'film1', 'actor1') \
        .expand('actor1', [('dbpp:birthPlace', 'actor_country1'), ('dbpp:name', 'name')]) \
        .filter({'actor_country1': ['regex(str(?actor_country1), "USA")']}).group_by(['name']).count('film1')

    yago_actors = graph2.feature_domain_range('yago:actedIn', 'actor2', 'film2') \
        .expand('actor2', [('yago:isCitizenOf', 'actor_country2'), ('yagoinfo:name', 'name')]) \
        .filter({'actor_country2': ['= yago:United_States']}).group_by(['name']).count('film2')

    actors = dbpedia_actors.join(yago_actors, 'name', join_type=join_type)
    print(actors.to_sparql())

    #df = actors.execute(client, return_format=output_format)
    #print(df.shape)


join_grouped_expandable(JoinType.OuterJoin)
#join_warning(JoinType.InnerJoin)

"""
Inner Join
PREFIX  dbpp: <http://dbpedia.org/property/>
PREFIX  dbpo: <http://dbpedia.org/ontology/>
PREFIX  rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX  yagoinfo: <http://yago-knowledge.org/resource/infobox/en/>
PREFIX  yago: <http://yago-knowledge.org/resource/>
PREFIX  dcterms: <http://purl.org/dc/terms/>
PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  dbpr: <http://dbpedia.org/resource/>

SELECT *
WHERE
  { GRAPH <http://dbpedia.org>
      { ?film1   dbpp:starring    ?actor1 .
        ?actor1  dbpp:birthPlace  ?actor_country1 ;
                dbpp:name        ?name
        FILTER regex(str(?actor_country1), "USA")
      }
    GRAPH <http://yago-knowledge.org/>
      { ?actor2  yago:actedIn      ?film2 ;
                yago:isCitizenOf  ?actor_country2 ;
                yagoinfo:name     ?name
        FILTER ( ?actor_country2 = yago:United_States )
      }
  }
# 37569 Rows. -- 17215 msec.
Unique names = 454 Rows. -- 661 msec.
"""


"""
Left Outer Join
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
      { ?film1   dbpp:starring    ?actor1 .
        ?actor1  dbpp:birthPlace  ?actor_country1 ;
                 dbpp:name        ?name
        FILTER regex(str(?actor_country1), "USA")
      }
    OPTIONAL
      { GRAPH <http://yago-knowledge.org/>
          { ?actor2  yago:actedIn      ?film2 ;
                     yago:isCitizenOf  ?actor_country2 ;
                     yagoinfo:name     ?name
            FILTER ( ?actor_country2 = yago:United_States )
          }
      }
  }
38675 Rows. -- 17509 msec.
Unique names: 848 Rows. -- 844 msec.
"""


"""
Right Outer Join
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dbpp: <http://dbpedia.org/property/>
PREFIX dbpr: <http://dbpedia.org/resource/>
PREFIX dbpo: <http://dbpedia.org/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX yago: <http://yago-knowledge.org/resource/>
PREFIX yagoinfo: <http://yago-knowledge.org/resource/infobox/en/>
SELECT DISTINCT ?name
WHERE {
	GRAPH <http://yago-knowledge.org/> { 
		?actor2 yago:actedIn ?film2 .
			?actor2 yago:isCitizenOf ?actor_country2 .
			?actor2 yagoinfo:name ?name .
			FILTER (  (?actor_country2 = yago:United_States ) ) 
			}
	OPTIONAL { GRAPH <http://dbpedia.org> { 
		?film1 dbpp:starring ?actor1 .
			?actor1 dbpp:birthPlace ?actor_country1 .
			?actor1 dbpp:name ?name .
			FILTER ( regex(str(?actor_country1), "USA") ) 
			}}
	}
140225 Rows. -- 20522 msec.
Unique names 14,140 Rows. -- 204 msec.
"""

"""
Grouped_Expandable_Inner_Join
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
  { GRAPH <http://yago-knowledge.org/>
      { ?actor2  yago:actedIn      ?film2 ;
                 yago:isCitizenOf  ?actor_country2 ;
                 yagoinfo:name     ?name
        FILTER ( ?actor_country2 = yago:United_States )
      }
    GRAPH <http://dbpedia.org>
      { { SELECT DISTINCT  ?name (COUNT(?film1) AS ?count)
          WHERE
            { ?film1   dbpp:starring    ?actor1 .
              ?actor1  dbpp:birthPlace  ?actor_country1 ;
                       dbpp:name        ?name
              FILTER regex(str(?actor_country1), "USA")
            }
          GROUP BY ?name
        }
      }
  }
"""


"""
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dbpp: <http://dbpedia.org/property/>
PREFIX dbpr: <http://dbpedia.org/resource/>
PREFIX dbpo: <http://dbpedia.org/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX yago: <http://yago-knowledge.org/resource/>
PREFIX yagoinfo: <http://yago-knowledge.org/resource/infobox/en/>
SELECT * 
FROM <http://dbpedia.org>
WHERE {
	{
	SELECT DISTINCT ?name  (COUNT( ?film1) AS ?count) 
	WHERE {
		?film1 dbpp:starring ?actor1 .
		?actor1 dbpp:birthPlace ?actor_country1 .
		?actor1 dbpp:name ?name .
		FILTER ( regex(str(?actor_country1), "USA") ) 
		} GROUP BY ?name 
	}
	UNION
	{
	SELECT DISTINCT ?name  (COUNT( ?film2) AS ?count) 
	WHERE {
		?actor2 yago:actedIn ?film2 .
		?actor2 yago:isCitizenOf ?actor_country2 .
		?actor2 yagoinfo:name ?name .
		FILTER (  (?actor_country2 = yago:United_States ) ) 
		} GROUP BY ?name 
	}
	}

"""
