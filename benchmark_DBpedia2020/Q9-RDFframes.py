''' Get the list of basketball players in DBpedia, their teams, and the number of players in each team. ''' 

from rdfframes.knowledge_graph import KnowledgeGraph


graph = KnowledgeGraph(graph_name='dbpedia')


def basket_ball_teams_player_count():
	players = graph.entities('dbpo:BasketballPlayer', entities_col_name='player')\
		.expand('player', [('foaf:name', 'name'), ('dbpp:team', 'team'), ('dbpo:position', 'position'), ('dbpp:birthPlace', 'country'),('dbpo:careerStation','career')])\
		.group_by(['team']).count('player', 'count_players', True)
	print(players.to_sparql())


basket_ball_teams_player_count()
