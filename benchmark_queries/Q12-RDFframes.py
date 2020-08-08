from rdfframes.knowledge_graph import KnowledgeGraph


graph = KnowledgeGraph(graph_name='dbpedia')


def expand_groupby_expand():
    basketball_palyer = graph.entities('dbpo:Athlete', entities_col_name='player')\
        .expand('player', [('dbpp:team', 'team')])\
        .group_by(['team']).count('player', 'count_basketball_players', True)\
        .expand('team', [('dbpp:name', 'name')])
    print("SPARQL QUERY \n{}\n".format(basketball_palyer.to_sparql()))


expand_groupby_expand()
