''' 
Get the nationality, place, date of birth for each basketball player, in addition to the sponsor, name, and president of his team (if available).
'''

from time import time
from rdfframes.knowledge_graph import KnowledgeGraph
from rdfframes.utils.constants import JoinType
from rdfframes.client.http_client import HttpClientDataFormat, HttpClient

graph =  KnowledgeGraph(graph_name='dbpedia')


def expand_join(join_type):
    basketball_palyer = graph.entities('dbpo:BasketballPlayer', entities_col_name='player')\
      .expand('player', [('dbpp:nationality', 'nationality') ,('dbpp:birthPlace', 'place'),
                         ('dbpp:birthDate','birthDate'),('dbpp:team', 'team')])
    basketball_team = graph.entities('dbpo:BasketballTeam', entities_col_name='team')\
      .expand('team', [('dbpp:president', 'president'), ('dbpp:sponsor', 'sponsor'), ('dbpp:name', 'name')])
    basketball_palyer_team = basketball_palyer.join(basketball_team,'team', join_type=join_type)
    print(basketball_palyer_team.to_sparql()


expand_join(JoinType.LeftOuterJoin)
