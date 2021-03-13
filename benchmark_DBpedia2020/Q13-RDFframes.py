
''' Get the sponsor, name, president, and the number of basketball players in each basketball team. '''

from rdfframes.knowledge_graph import KnowledgeGraph
from rdfframes.utils.constants import JoinType
from rdfframes.client.http_client import HttpClientDataFormat, HttpClient
from time import time
from rdfframes.dataset.rdfpredicate import PredicateDirection

graph =    KnowledgeGraph(graph_name='dbpedia')

def expand_groupby_join(join_type):
    basketball_palyer = graph.entities('dbpo:BasketballPlayer', entities_col_name='player')\
	.expand('player', [('dbpp:team', 'team')])\
	.group_by(['team']).count('player', 'count_basketball_players', True)

    basketball_team = graph.entities('dbpo:BasketballTeam', entities_col_name='team')\
    .expand('team', [('dbpo:league', 'league'), ('dbpp:location', 'location'), ('dbpp:name', 'name')])
    basketball_palyer_team = basketball_team.join(basketball_palyer,'team', join_type=join_type)
    print("SPARQL QUERY FOR JOIN TYPE {} \n{}\n".format(join_type, basketball_palyer_team.to_sparql()))
    

expand_groupby_join(JoinType.InnerJoin)

