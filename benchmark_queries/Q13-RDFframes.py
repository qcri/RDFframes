'''    Q13 & Get a list of films in DBpedia. For each film, return the actor, language, country, genre, story, studio in addition to the director, producer, and title (if available). '''
from rdfframes.knowledge_graph import KnowledgeGraph


graph = KnowledgeGraph(graph_name='dbpedia')


def expand_optional():
    Films = graph.entities('dbpo:Film', entities_col_name='film')\
        .expand('film', [('dbpp:starring', 'actor'), ('dbpp:director','director', True),
                         ('dbpp:country', 'movie_country'), ('dbpp:producer', 'producer', True),
                         ('dbpp:language', 'language'), ('dbpp:story','story'),
                         ('dbpp:runtime', 'runtime'), ('dbpp:studio' ,'studio', True), ('dbpp:title', 'title'),
                         ('dbpp:genre', 'genre')])
    print("SPARQL Query = \n{}".format(Films.to_sparql()))


expand_optional()
