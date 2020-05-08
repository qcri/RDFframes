from rdfframes.knowledge_graph import KnowledgeGraph


def kge():
    graph = KnowledgeGraph(graph_uri='http://dblp.13s.de/')
    triples = graph.feature_domain_range("pred", domain_col_name='sub', range_col_name='obj')\
        .filter({'obj': ['isIRI(?obj)']})

    print(triples.to_sparql())

kge()