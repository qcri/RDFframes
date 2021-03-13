''' Get the actor, director, producer, language of the list of films in
    DBpedia that produced by any studio in India or the United States excluding `Eskay Movies' studio,
    and has one of the following genres (Film score, Soundtrack, Rock music, House music, or Dubstep). '''

from rdfframes.knowledge_graph import KnowledgeGraph
from rdfframes.client.http_client import HttpClientDataFormat, HttpClient
from time import time

graph = KnowledgeGraph(graph_name='dbpedia')


def expand_filter_expand():
    films = graph.entities('dbpo:Film', entities_col_name='film')\
        .expand('film', [('dbpp:starring', 'actor'), ('dbpp:country', 'movie_country'), ('dbpp:genre', 'genre'),
                         ('dbpp:director','director'), ('dbpp:producer', 'producer'), ('dbpp:language', 'language'),
                         ('dbpp:story','story'), ('dbpp:runtime', 'runtime'), ('dbpp:studio','studio'),
                          ('dbpp:title', 'title')])\
        .filter({'genre': ['IN (dbpr:Film_score, dbpr:Soundtrack, dbpr:Rock_music, dbpr:House_music, dbpr:Dubstep)']})\
        .filter({'studio': ['!= "Eskay Movies"']})\
        .filter({'movie_country': [' IN (dbpr:United_States, dbpr:India)']})
    #print(films.to_sparql())
   

expand_filter_expand()
  

