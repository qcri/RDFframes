from time import time
from rdfframes.knowledge_graph import KnowledgeGraph
from rdfframes.client.http_client import HttpClientDataFormat, HttpClient
from rdfframes.client.sparql_endpoint_client import SPARQLEndpointClient

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
graph = KnowledgeGraph(graph_name='dbpedia')


def basket_ball_teams_player_count():
	players = graph.entities('dbpo:BasketballPlayer', entities_col_name='player')\
		.expand('player', [('dbpp:team', 'team'), ('dbpp:years', 'years'), ('dbpo:termPeriod', 'period')])\
		.group_by(['team']).count('player', 'count_players', True)
	df = players.execute(client)
	#print(df.shape)

start = time()
basket_ball_teams_player_count()
duration = time()-start


"""
Optimized SPARQL query for basket_ball_teams_player_count on isql
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dbpp: <http://dbpedia.org/property/>
PREFIX dbpr: <http://dbpedia.org/resource/>
PREFIX dbpo: <http://dbpedia.org/ontology/>
SELECT DISTINCT ?team	(COUNT(DISTINCT ?player) AS ?count_players) 
FROM <http://dbpedia.org>
WHERE {
	?player rdf:type dbpo:BasketballPlayer .
	?player dbpp:team ?team .
	?player dbpp:years ?years .
	?player dbpo:termPeriod ?period .
	} GROUP BY ?team 
# 8352 Rows. -- 3537 msec.
# 8352 Rows. -- 3563 msec.
# 8352 Rows. -- 3594 msec.
# 8352 Rows. -- 3906 msec.
"""


"""
Naive query for basket_ball_teams_player_count on isql
PREFIX	dbpp: <http://dbpedia.org/property/>
PREFIX	dbpo: <http://dbpedia.org/ontology/>
PREFIX	rdf:	<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX	dcterms: <http://purl.org/dc/terms/>
PREFIX	rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX	dbpr: <http://dbpedia.org/resource/>

SELECT	?team ?count_players
FROM <http://dbpedia.org>
WHERE
	{ { SELECT DISTINCT	?team (COUNT(DISTINCT ?player) AS ?count_players)
			WHERE
				{ SELECT DISTINCT ?player ?team ?years ?period WHERE {
				 { SELECT	?player
						WHERE
							{ ?player	rdf:type	dbpo:BasketballPlayer }
					}
					{ SELECT	?player ?team
						WHERE
							{ ?player	dbpp:team	?team }
					}
					{ SELECT	?player ?years
						WHERE
							{ ?player	dbpp:years	?years }
					}
					{ SELECT	?player ?period
						WHERE
							{ ?player	dbpo:termPeriod	?period }
					}
				}
				} GROUP BY ?team
		}
	}
# 8352 Rows. -- 11343 msec.
# 8352 Rows. -- 11781 msec.
# 8352 Rows. -- 11967 msec.
"""
