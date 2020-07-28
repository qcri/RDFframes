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


def expand_groupby_columns():
    Films = graph.entities('dbpo:Film', entities_col_name='film')\
    .expand('film', [('dbpp:starring', 'actor'), ('dbpp:director','director', True), ('dbpp:country', 'movie_country'), 
      ('dbpp:producer', 'producer', True), ('dbpp:language', 'language'), ('dbpp:story','story') ,
      ('dbpp:runtime', 'runtime'), ('dbpp:studio' ,'studio', True), ('dbpp:title', 'title'),
      ('dbpp:genre', 'genre')])\
      .group_by(['movie_country','genre']).count('film', 'film_country_genre', True)

    ######
    basketball_palyer = graph.entities('dbpo:BasketballPlayer', entities_col_name='player')\
		.expand('player', [('dbpp:birthPlace', 'place'), ('dbpp:team', 'team')])\
		.group_by(['place','team']).count('player', 'count_basketball_players', True)
    df = Films.execute(client)
    print(Films.to_sparql())
    print(df.shape)
    #print(df)


start = time()
expand_groupby_columns()
expand_groupby_columns()
duration = time()-start
print("Duration of join = {} sec".format(duration))


