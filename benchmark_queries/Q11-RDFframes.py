from rdfframes.knowledge_graph import KnowledgeGraph
from rdfframes.utils.constants import JoinType


graph1 =  KnowledgeGraph(graph_name='dbpedia')
graph2 = KnowledgeGraph(graph_name='yago',
                        graph_uri='http://yago-knowledge.org/',
                        prefixes={
                            'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
                            'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
                            'yago': 'http://yago-knowledge.org/resource/',
                            'yagoinfo': 'http://yago-knowledge.org/resource/infobox/en/'
                        })


def join(join_type):
    dbpedia_actors = graph1.feature_domain_range('dbpp:starring', 'film1', 'actor1') \
        .expand('actor1', [('dbpp:birthPlace', 'actor_country1'), ('dbpp:name', 'name')]) \
        .filter({'actor_country1': ['regex(str(?actor_country1), "USA")']})

    yago_actors = graph2.feature_domain_range('yago:actedIn', 'actor2', 'film2') \
        .expand('actor2', [('yago:isCitizenOf', 'actor_country2'), ('yagoinfo:name', 'name')]) \
        .filter({'actor_country2': ['= yago:United_States']})

    actors = dbpedia_actors.join(yago_actors, 'name', join_type=join_type)
    print(actors.to_sparql())


join(JoinType.OuterJoin)








