
''' Get the list of films in DBpedia that belong to the same genre and produced in the same country. For each film in each list,
return the actors, country, story, language, genre, studio in addition to the director, producer, or title (if available). '''


from rdfframes.knowledge_graph import KnowledgeGraph


graph = KnowledgeGraph(graph_name='dbpedia')


def expand_groupby_columns():
    films = graph.entities('dbpo:Film', entities_col_name='film')\
        .expand('film', [('dbpp:starring', 'actor'), ('dbpp:director','director', True),
                         ('dbpp:country', 'movie_country'), ('dbpp:producer', 'producer', True),
                         ('dbpp:language', 'language'), ('dbpp:story','story'), ('dbpp:runtime', 'runtime'),
                         ('dbpp:studio' ,'studio', True), ('dbpp:title', 'title'), ('dbpp:genre', 'genre')])\
        .group_by(['movie_country','genre']).count('film', 'film_country_genre', True)
    print(films.to_sparql())


expand_groupby_columns()


