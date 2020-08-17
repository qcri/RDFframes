''' Get the actor, language, and (producer, director, title if available) of the list of films in DBpedia that produced 
    by any studio in India or the United States excluding `Eskay Movies' studio, and has one of the following genres 
    (Film score, Soundtrack, Rock music, House music, or Dubstep). '''

from rdfframes.knowledge_graph import KnowledgeGraph


graph =  KnowledgeGraph(graph_name='dbpedia')


def optional_expand_filter_expand():
    Films = graph.entities('dbpo:Film', entities_col_name='film')\
        .expand('film', [('dbpp:starring', 'actor'), ('dbpp:country', 'movie_country')])\
        .filter({'movie_country': [' IN (dbpr:United_States, dbpr:India)']})\
        .expand('film', [ ('dbpp:genre', 'genre')])\
        .expand('film', [ ('dbpp:director','director', True), ('dbpp:producer', 'producer', True),
                          ('dbpp:language', 'language'), ('dbpp:story','story'), ('dbpp:studio' ,'studio'),
                          ('dbpp:title', 'title', True)])\
        .filter({'genre': ['IN (dbpr:Film_score, dbpr:Soundtrack, dbpr:Rock_music, dbpr:House_music, dbpr:Dubstep)']})\
        .filter({'studio': ['!= "Eskay Movies"']})
    print("SPARQL Query = \n{}".format(Films.to_sparql()))


optional_expand_filter_expand()
