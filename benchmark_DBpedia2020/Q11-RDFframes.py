
''' Get the list of athletes in DBpedia. For each athlete, return his birthplace and the number of players who were born in the same place. '''

from rdfframes.knowledge_graph import KnowledgeGraph


graph =  KnowledgeGraph(graph_name='dbpedia')


def expand_groupby_expand():
    basketball_palyer = graph.entities('dbpo:Athlete', entities_col_name='player')\
        .expand('player', [('dbpp:birthPlace', 'place'), ('dbpp:team', 'team')])\
        .group_by(['place']).count('player', 'count_basketball_players', True).count('team', 'count_basketball_teams', True)
    print(basketball_palyer.to_sparql())

expand_groupby_expand()
