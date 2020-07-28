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


def expand_filter_expand():
  Films = graph.entities('dbpo:Film', entities_col_name='film')\
    .expand('film', [('dbpp:starring', 'actor'), ('dbpp:country', 'movie_country')])\
    .filter({'movie_country': [' IN (dbpr:United_States, dbpr:India)']})\
	.expand('film', [ ('dbpp:genre', 'genre')])\
	.expand('film', [ ('dbpp:director','director'), ('dbpp:producer', 'producer'), ('dbpp:language', 'language'), ('dbpp:story','story') ,
      ('dbpp:runtime', 'runtime'), ('dbpp:studio' ,'studio'), ('dbpp:title', 'title')])\
    .filter({'genre': ['IN (dbpr:Film_score, dbpr:Soundtrack, dbpr:Rock_music, dbpr:House_music, dbpr:Dubstep)']})\
    .filter({'studio': ['!= "Eskay Movies"']})
  df = Films.execute(client)
  print(Films.to_sparql())



start_time = time.time()
expand_filter_expand()
end_time = time.time()
