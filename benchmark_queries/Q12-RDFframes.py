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
  print("SPARQL QUERY \n{}\n".format(basketball_palyer.to_sparql()))
  df = basketball_palyer.execute(client)
  #print(df.shape)

start = time()
expand_groupby_expand()
duration = time()-start
print("Duration of groupby 2 columns = {} sec".format(duration))

