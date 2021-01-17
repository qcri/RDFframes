
''' Get information about the Films in DBpedia: actor, director, country, producer,
   language, title, genre, story, studio . Filter on country, studio and genre, runtime. '''


from rdfframes.knowledge_graph import KnowledgeGraph


graph = KnowledgeGraph(graph_name='dbpedia')


def expand_filter_expand():
    films = graph.entities('dbpo:Film', entities_col_name='film')\
        .expand('film', [('dbpp:starring', 'actor'), ('dbpp:country', 'movie_country')])\
        .filter({'movie_country': [' IN (dbpr:United_States, dbpr:India)']})\
        .expand('film', [ ('dbpp:genre', 'genre')])\
        .expand('film', [ ('dbpp:director','director'), ('dbpp:producer', 'producer'), ('dbpp:language', 'language'),
                          ('dbpp:story','story') ,('dbpp:runtime', 'runtime'), ('dbpp:studio' ,'studio'),
                          ('dbpp:title', 'title')])\
        .filter({'genre': ['IN (dbpr:Film_score, dbpr:Soundtrack, dbpr:Rock_music, dbpr:House_music, dbpr:Dubstep)']})\
        .filter({'studio': ['!= "Eskay Movies"']})
    print(films.to_sparql())


expand_filter_expand()
