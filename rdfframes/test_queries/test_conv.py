import time

from rdfframes.knowledge_graph import KnowledgeGraph
from rdfframes.client.http_client import HttpClient, HttpClientDataFormat


def test_convenience_functions():
    graph = KnowledgeGraph(graph_name='dbpedia')
    entities = graph.entities('dbpo:BasketballPlayer', entities_col_name='player')
    print(entities.to_sparql())
    features = graph.features('dbpo:BasketballPlayer', features_col_name='feature_uri')
    print(features.to_sparql())
    entities_feats = graph.entities_and_features('dbpo:BasketballPlayer', [('dbpp:nationality', 'nationality'),
                                                                           ('dbpp:birthPlace', 'place'),
                                                                           ('dbpp:birthDate', 'birthDate'),
                                                                           ('dbpp:team', 'team')])
    print(entities_feats.to_sparql())
    classes_freq = graph.classes_and_freq()
    print(classes_freq.to_sparql())
    feats_freq = graph.features_and_freq('dbpo:BasketballPlayer')
    print(feats_freq.to_sparql())
    n_entities = graph.num_entities('dbpo:BasketballPlayer')
    print(n_entities.to_sparql())


if __name__ == '__main__':
    test_convenience_functions()