import unittest

from rdfframes.knowledge_graph import KnowledgeGraph
from rdfframes.utils.constants import JoinType


class TestJoins(unittest.TestCase):

    def test_expandable_expandable_inner_join(self):
        true_sparql = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" \
                      "PREFIX sioc: <http://rdfs.org/sioc/ns#>" \
                      "PREFIX sioct: <http://rdfs.org/sioc/types#>" \
                      "PREFIX to: <http://twitter.com/ontology/>" \
                      "PREFIX dcterms: <http://purl.org/dc/terms/>" \
                      "PREFIX xsd: <http://www.example.org/>" \
                      "PREFIX foaf: <http://xmlns.com/foaf/0.1/>" \
                      "FROM <https://twitter.com>" \
                      "SELECT ?tweep ?name " \
                      "WHERE {" \
                      " ?tweet rdf:type sioct:microblogPost ." \
                      " ?tweet sioc:has_creater ?tweep ." \
                      " ?tweet sioc:content ?text ." \
                      " ?tweep rdf:type sioct:tweeter . " \
                      " ?tweep sioc:has_name ?name ." \
                      "}"
        generated_sparql = self._expandable_expandable_join(JoinType.InnerJoin, False, False)
        self.assertEqual(''.join(true_sparql.split()), ''.join(generated_sparql.split()))

    def test_expandable_expandable_inner_join_with_optional(self):
        true_sparql = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" \
                      "PREFIX sioc: <http://rdfs.org/sioc/ns#>" \
                      "PREFIX sioct: <http://rdfs.org/sioc/types#>" \
                      "PREFIX to: <http://twitter.com/ontology/>" \
                      "PREFIX dcterms: <http://purl.org/dc/terms/>" \
                      "PREFIX xsd: <http://www.example.org/>" \
                      "PREFIX foaf: <http://xmlns.com/foaf/0.1/>" \
                      "FROM <https://twitter.com>" \
                      "SELECT ?tweep ?name " \
                      "WHERE {" \
                      " ?tweet rdf:type sioct:microblogPost ." \
                      " ?tweet sioc:has_creater ?tweep ." \
                      " ?tweep rdf:type sioct:tweeter . " \
                      " OPTIONAL {" \
                      " ?tweet sioc:content ?text ." \
                      " ?tweep sioc:has_name ?name .}" \
                      "}"
        generated_sparql = self._expandable_expandable_join(JoinType.InnerJoin, True, True)
        self.assertEqual(''.join(true_sparql.split()), ''.join(generated_sparql.split()))

    def test_expandable_expandable_leftouter_join(self):
        true_sparql = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" \
                      "PREFIX sioc: <http://rdfs.org/sioc/ns#>" \
                      "PREFIX sioct: <http://rdfs.org/sioc/types#>" \
                      "PREFIX to: <http://twitter.com/ontology/>" \
                      "PREFIX dcterms: <http://purl.org/dc/terms/>" \
                      "PREFIX xsd: <http://www.example.org/>" \
                      "PREFIX foaf: <http://xmlns.com/foaf/0.1/>" \
                      "FROM <https://twitter.com>" \
                      "SELECT ?tweep ?name " \
                      "WHERE {" \
                      " ?tweet rdf:type sioct:microblogPost ." \
                      " ?tweet sioc:has_creater ?tweep ." \
                      " ?tweet sioc:content ?text ." \
                      " OPTIONAL {" \
                      " ?tweep rdf:type sioct:tweeter . " \
                      " ?tweep sioc:has_name ?name .}" \
                      "}"
        generated_sparql = self._expandable_expandable_join(JoinType.LeftOuterJoin, False, False)
        self.assertEqual(''.join(true_sparql.split()), ''.join(generated_sparql.split()))


    @staticmethod
    def _expandable_expandable_join(join_type, optional1, optional2):
        # create a knowledge graph to store the graph uri and prefixes
        graph = KnowledgeGraph('twitter', 'https://twitter.com',
                               prefixes={
                                   "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                                   "sioc": "http://rdfs.org/sioc/ns#",
                                   "sioct": "http://rdfs.org/sioc/types#",
                                   "to": "http://twitter.com/ontology/",
                                   "dcterms": "http://purl.org/dc/terms/",
                                   "xsd": "http://www.example.org/",
                                   "foaf": "http://xmlns.com/foaf/0.1/"
                               })
        # return all the instances of the tweet class
        dataset = graph.entities(class_name='sioct:microblogPost',
                                 new_dataset_name='dataset1',
                                 entities_col_name='tweet')
        dataset = dataset.expand(src_col_name='tweet', predicate_list=[
            ('sioc:has_creater', 'tweep', False),
            ('sioc:content', 'text', optional1)
        ]).select_cols(['tweep'])

        dataset2 = graph.entities(class_name='sioct:tweeter',
                                 new_dataset_name='dataset2',
                                 entities_col_name='tweeter')
        dataset2 = dataset2.expand(src_col_name='tweeter', predicate_list=[
            ('sioc:has_name', 'name', optional2)
        ])

        dataset.join(dataset2,'tweep','tweeter','tweep', join_type)

        return dataset.to_sparql()


if __name__ == '__main__':
    unittest.main()

