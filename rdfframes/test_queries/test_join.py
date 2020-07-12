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
  print("SPARQL QUERY FOR JOIN TYPE {} \n{}\n".format(join_type, basketball_palyer_team.to_sparql()))
  #df = basketball_palyer_team.execute(client)
  #print(basketball_palyer_team.to_sparql())
  #df = dataset.execute(client, return_format=output_format)
  #print(df.shape)

def groupby_expand_join(join_type):
  basketball_palyer = graph.entities('dbpo:BasketballPlayer', entities_col_name='player')\
	.expand('player', [('dbpp:team', 'team')])\
	.group_by(['team']).count('player', 'count_basketball_players', True)

  basketball_team = graph.entities('dbpo:BasketballTeam', entities_col_name='team')\
  .expand('team', [('dbpp:president', 'president'), ('dbpp:sponsor', 'sponsor'), ('dbpp:name', 'name')])
  basketball_palyer_team = basketball_palyer.join(basketball_team,'team', join_type=join_type)
  print("SPARQL QUERY FOR JOIN TYPE {} \n{}\n".format(join_type, basketball_palyer_team.to_sparql()))


def expand_join(join_type):
  basketball_palyer = graph.entities('dbpo:BasketballPlayer', entities_col_name='player')\
		.expand('player', [('dbpp:nationality', 'nationality') ,('dbpp:birthPlace', 'place')\
            ,('dbpp:birthDate','birthDate'),('dbpp:team', 'team')])
  basketball_team = graph.entities('dbpo:BasketballTeam', entities_col_name='team')\
  .expand('team', [('dbpp:president', 'president'), ('dbpp:sponsor', 'sponsor'), ('dbpp:name', 'name')])
  basketball_palyer_team = basketball_team.join(basketball_palyer,'team', join_type=join_type)
  print(basketball_palyer_team.to_sparql())
  #df = basketball_palyer_team.execute(client)


def group_join(join_type):
    basket_ball = graph.entities('dbpo:BasketballPlayer', entities_col_name='player') \
        .expand('player', [('dbpp:birthPlace', 'place')]) \
        .group_by(['place']).count('player', 'count_basketball_players', True)

    tennis = graph.entities('dbpo:TennisPlayer', entities_col_name='player') \
        .expand('player', [('dbpp:birthPlace', 'place')]) \
        .group_by(['place']).count('player', 'count_tennis_players', True)
    teams = basket_ball.join(tennis, 'place', join_type=join_type)
    print(teams.to_sparql())


start = time()
expand_groupby_join(JoinType.InnerJoin)
duration = time()-start
print("Duration of Inner join on expandable grouped datasets = {} sec".format(duration))

start = time()
groupby_expand_join(JoinType.InnerJoin)
duration = time()-start
print("Duration of Inner join on grouped expandable datasets = {} sec".format(duration))



start = time()
expand_groupby_join(JoinType.LeftOuterJoin) ## change the type here.
duration = time()-start
print("Duration of LeftOuter Join on expandable grouped datasets = {} sec".format(duration))


start = time()
groupby_expand_join(JoinType.LeftOuterJoin) ## change the type here.
duration = time()-start
print("Duration of LeftOuter Join on grouped expandable datasets = {} sec".format(duration))




start = time()
expand_groupby_join(JoinType.RightOuterJoin) ## change the type here.
duration = time()-start
print("Duration of RightOuter Join on expandable grouped datasets = {} sec".format(duration))


start = time()
groupby_expand_join(JoinType.RightOuterJoin) ## change the type here.
duration = time()-start
print("Duration of RightOuter Join on grouped expandable  datasets = {} sec".format(duration))


start = time()
expand_groupby_join(JoinType.OuterJoin) ## change the type here.
duration = time()-start
print("Duration of Outer join on expandable grouped datasets = {} sec".format(duration))

start = time()
groupby_expand_join(JoinType.OuterJoin) ## change the type here.
duration = time()-start
print("Duration of Outer join on grouped expandable  datasets = {} sec".format(duration))


start = time()
expand_join(JoinType.InnerJoin) ## change the type here.
duration = time()-start
print("Duration of Inner join on expandable datasets = {} sec".format(duration))


start = time()
expand_join(JoinType.LeftOuterJoin) ## change the type here.
duration = time()-start
print("Duration of LeftOuter Join on expandable datasets = {} sec".format(duration))



start = time()
expand_join(JoinType.RightOuterJoin) ## change the type here.
duration = time()-start
print("Duration ofRightOuter Join on expandable datasets = {} sec".format(duration))



start = time()
expand_join(JoinType.OuterJoin) ## change the type here.
duration = time()-start
print("Duration of Outer join on expandable datasets = {} sec".format(duration))


start = time()
group_join(JoinType.InnerJoin) ## change the type here.
duration = time()-start
print("Duration of Inner join on expandable datasets = {} sec".format(duration))


start = time()
group_join(JoinType.LeftOuterJoin) ## change the type here.
duration = time()-start
print("Duration of LeftOuter Join on expandable datasets = {} sec".format(duration))



start = time()
group_join(JoinType.RightOuterJoin) ## change the type here.
duration = time()-start
print("Duration ofRightOuter Join on expandable datasets = {} sec".format(duration))



start = time()
group_join(JoinType.OuterJoin) ## change the type here.
duration = time()-start
print("Duration of Outer join on expandable datasets = {} sec".format(duration))
