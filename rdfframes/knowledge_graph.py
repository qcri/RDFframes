from rdfframes.dataset.expandable_dataset import ExpandableDataset
from rdfframes.dataset.rdfpredicate import PredicateDirection


__author__ = """
Abdurrahman Ghanem <abghanem@hbku.edu.qa>
Aisha Mohamed <ahmohamed@qf.org.qa>
Ghadeer AbuOda <gabuoda@qf.org.qa>
"""


class KnowledgeGraph:
    """
    High level class represents one or more knowledge graphs (URIs). It
    contains a group of convenience functions to initialize datasets before 
    applying any operations on them
    """
    default_graphs = {'dbpedia': 'http://dbpedia.org',
             'dblp': 'http://dblp.l3s.de'}
    default_graph_prefixes = {
        'dbpedia': {
            'dcterms': 'http://purl.org/dc/terms/',
            'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
            'dbpp': 'http://dbpedia.org/property/',
            'dbpr': 'http://dbpedia.org/resource/',
            'dbpo': 'http://dbpedia.org/ontology/'},
        'dblp': {
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "swrc": "http://swrc.ontoware.org/ontology#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "dc": "http://purl.org/dc/elements/1.1/",
            "dcterm": "http://purl.org/dc/terms/",
            "dblprc": "http://dblp.l3s.de/d2r/resource/conferences/"}
        }

    def __init__(self, graph_name=None, graph_uri=None, prefixes=None):
        """
        Initializes the object with one graph. Other graphs can be added using
        add_graph method.
        :param graph_name: graph user defined name
        :type graph_name: string
        :param graph_uri: graph URI
        :type graph_uri: string
        :param prefixes: a dictionary of the prefixes to use in this graph. Keys
            are the prefixes and values are the URIs.
        :type prefixes: a dictionary where the key and value are strings.
        """
        self.graphs = {}
        self.graph_prefixes = {}
        self.add_graph(graph_name, graph_uri, prefixes)

    def add_graph(self, graph_name=None, graph_uri=None, prefixes=None):
        """
        add more knowledge graph URIs to this KnowledgeGraph instance
        :param graph_name: graph user defined name
        :type graph_name: string
        :param graph_uri: graph URI
        :type graph_uri: string
        :param prefixes: a dictionary of the prefixes to use in this graph. Keys
            are the prefixes and values are the URIs.
        :type prefixes: a dictionary where the key and value are strings.
        :return:
        """
        if graph_name is not None:
            if len(graph_name) <= 0:
                raise Exception("Graph name cannot be an empty string.")
            if graph_uri is not None:
                self.graphs[graph_name] = graph_uri
                if prefixes is not None:
                    self.__add_graph_prefixes(graph_name, prefixes)
                else:
                    self.__load_default_prefixes(graph_name)
            elif graph_name in KnowledgeGraph.default_graphs:
                self.graphs[graph_name] = KnowledgeGraph.default_graphs[graph_name]
                if graph_name in KnowledgeGraph.default_graph_prefixes:
                    self.__add_graph_prefixes(graph_name, KnowledgeGraph.default_graph_prefixes[graph_name])
                else:
                    self.__load_default_prefixes(graph_name)
            else:
                raise Exception("Graph is not one of the default graphs.")

        elif graph_uri is not None:
            graph_name = "graph{}".format(len(self.graphs))
            self.graphs[graph_name] = graph_uri
            if prefixes is not None:
                self.__add_graph_prefixes(graph_name, prefixes)
            else:
                self.__load_default_prefixes(graph_name)
        else:
            graph_name = ""
            if prefixes is not None:
                self.__add_graph_prefixes(graph_name, prefixes)
            else:
                self.__load_default_prefixes(graph_name)

    def __add_graph_prefixes(self, graph_name, graph_prefixes):
        """
        add prefixes to be used with the graph specified by graph_name
        :param graph_name: graph user defined name
        :type graph_name: string
        :param graph_prefixes: a dictionary of the prefixes to use in this graph. Keys
            are the prefixes and values are the prefix URIs. 
        :type graph_prefixes: a dictionary where the key and value are strings.
        :return:        
        """
        if graph_name not in self.graph_prefixes:
            self.graph_prefixes[graph_name] = {}

        for prefix, prefix_uri in graph_prefixes.items():
            if prefix not in self.graph_prefixes[graph_name]:
                self.graph_prefixes[graph_name][prefix] = prefix_uri

    def __load_default_prefixes(self, graph_name):
        """
        if no prefixes are given with the graph, load the default list of 
            prefixes to be used for this graph
        :param graph_name: graph name
        :type graph_name: string
        :return:
        """
        default_prefixes = {
            "foaf": "http://xmlns.com/foaf/0.1/",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "xtypes": "http://purl.org/xtypes/",
            "dcterms": "http://purl.org/dc/terms/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "dc": "http://purl.org/dc/elements/1.1/",
        }
        self.__add_graph_prefixes(graph_name, default_prefixes)

    def entities(self, class_name, new_dataset_name='dataset', entities_col_name='entity'):
        """
        Retrieves all entities in the predefined graphs whose type is the passed class_name.
        Equivalent to the following sparql query:
            select distinct ?e
            where {
                ?e  type ?class_class
            }
        :param class_name: the name of the class
        :type class_name: string
        :param new_dataset_name: the name of the created dataset holding the entities
        :type new_dataset_name: string
        :param entities_col_name: entities column name in the returned dataset
        :type entities_col_name: string
        :return: new dataset with one column of the URIs entities of the class
        :rtype: Dataset
        """
        #return ExpandableDataset(self, new_dataset_name, class_name, "class") \
        #           .expand('class', [('rdf:type', entities_col_name, PredicateDirection.INCOMING)])
        #            .filter(conditions_dict={'class': ['= {}'.format(class_name)]})
        return ExpandableDataset(self, new_dataset_name, class_name, class_name) \
                    .expand(class_name, [
            ('rdf:type', entities_col_name, False, PredicateDirection.INCOMING)])

    def features(self, class_name, new_dataset_name='dataset', features_col_name='feature_uri'):
        """
        Retrieves all features in my graphs for all entities whose type is class_name
        Equivalent to the following sparql query:
        select distinct ?p
            where {
                ?e  type ?class.
                ?e  ?p   ?o.
            }
        :param new_dataset_name: the name of the created dataset holding the entities
        :type new_dataset_name: string
        :param class_name: class that are part of my graphs
        :type class_name: string
        :param features_col_name: features column name in the returned dataset
        :type features_col_name: string
        :return: new dataset with two columns mapping each class URI to the
            matching features
        :rtype: Dataset
        """
        return ExpandableDataset(self, new_dataset_name, class_name, class_name)\
            .expand(class_name, [('rdf:type', "entity", False, PredicateDirection.INCOMING)])\
            .expand("entity", [(features_col_name, "feature_value", False, PredicateDirection.OUTGOING)])

    def entities_and_features(self, class_name, features, new_dataset_name='dataset', entities_col_name='entity'):
        """
        Retrieves all entities in my graphs whose types are in the passed
        classes and their specified features.
        When an entity has two values for a  specific features, two rows are returned for the same entity.
        Equivalent to the following query:
        select ?e ?o1 ?o2 ..
        where {
                ?e  type ?class
                ?e ?p1 ?o1
                ?e ?p2 ?o2
                ..
        }
        :param new_dataset_name: the name of the created dataset holding the entities
        :type new_dataset_name: string
        :param class_name: class that are part of my graphs
        :type class_name: string
        :param features: a list 2-tuples (feature_uri, new_col_name) where each tuple represents a feature.
        :type features: a list of tuples of strings
        :param entities_col_name: entities column name in the returned dataset
        :type entities_col_name: string
        :return: new dataset with at least two columns mapping each class URI to
            the matching entities and their features
        :rtype: Dataset
        """
        ds = ExpandableDataset(self, new_dataset_name, class_name, class_name)\
            .expand(class_name, [('rdf:type', entities_col_name, False, PredicateDirection.INCOMING)])
        predicate_list = []
        for (pred_uri, col_name) in features:
            predicate_list.append((pred_uri, col_name, False, PredicateDirection.OUTGOING))
        ds.expand(entities_col_name, predicate_list)
        return ds

    def classes_and_freq(self, new_dataset_name='dataset', classes_col_name='class', frequency_col_name='frequency'):
        """
        retrieves all classes in my graph and their number of instances.
        Equivalent to the following query:
        select ?class count(distinct ?e)
            where {
                ?e  type ?class.
            }
            group by ?class
        :param new_dataset_name: the name of the created dataset holding the entities
        :type new_dataset_name: string
        :param classes_col_name: class column name in the returned dataset
        :type classes_col_name: string
        :param frequency_col_name: frequency column name in the returned dataset
        :type frequency_col_name: string
        :return: new dataset with two columns mapping each class URI to the
            number of entities of this type
        :rtype: Dataset 
        """
        return ExpandableDataset(self, new_dataset_name, "instance", "instance")\
            .expand("instance", [('rdf:type', classes_col_name, False, PredicateDirection.OUTGOING)])\
            .group_by([classes_col_name])\
            .count('instance', frequency_col_name)

    def features_and_freq(self, class_name, new_dataset_name='dataset', features_col_name="feature",
                          frequency_col_name='frequency'):
        """
        retrieves all features of the specified class and their frequency.
        equivalent to the following query:
        select ?class ?p count(distinct ?e)
        where {
            ?e  type ?class.
            ?e ?p ?o
        }
            group by ?class, ?p
        :param class_name: class that are part of my graphs
        :type class_name: string
        :param new_dataset_name: the name of the created dataset holding the entities
        :type new_dataset_name: string
        :param features_col_name: features column name in the returned dataset
        :type features_col_name: string
        :param frequency_col_name: frequency column name in the returned dataset
        :type frequency_col_name: string
        :return: new dataset with three columns mapping each class URI to the
            matching features and their frequency
        :rtype: Dataset
        """
        return ExpandableDataset(self, new_dataset_name, class_name, class_name)\
            .expand(class_name, [('rdf:type', 'instance', False, PredicateDirection.INCOMING)])\
            .expand('instance', [(features_col_name, 'feature_value', False, PredicateDirection.OUTGOING)])\
            .group_by([features_col_name]).\
            count('feature_value', frequency_col_name, unique=True)

    def num_entities(self, class_name, new_dataset_name='dataset', num_entities_col_name='num_entities'):
        """
        Counts all entities in the predefined graphs whose type is the passed classes.
        Equivalent to the following query:
        foreach class in classes:
            select ?class count(distinct ?e)
            where {
                ?e  type ?class
            }
        :param class_name: class that are part of my graphs
        :type class_name: string
        :param new_dataset_name: the name of the created dataset holding the entities
        :type new_dataset_name: string
        :param num_entities_col_name: count of entities column name in the
            returned dataset
        :type num_entities_col_name: string
        :return: new dataset with two columns mapping each class URI to the
            count of the matching entities
        :rtype: Dataset
        """
        return ExpandableDataset(self, new_dataset_name, class_name, class_name)\
            .expand(class_name, [('rdf:type', 'instance', False, PredicateDirection.INCOMING)])\
            .count('instance', num_entities_col_name, unique=True)

    def feature_domain_range(self, feature, domain_col_name="domain", range_col_name="range", new_dataset_name='dataset'):
        """
        retrieves all the subjects and objects of a given predicate. When graphs
        is passed, restrict to the specified graphs
        Equivalent to the query:
            select ?s ?o
            where {
                ?s  feature ?o
            }
        :param feature: feature to find its domain and range
        :type feature: string
        :param new_dataset_name: the name of the created dataset holding the entities
        :type new_dataset_name: string
        :param domain_col_name: name of domain column in the returned dataset
        :type domain_col_name: string
        :param range_col_name: name of range column in the returned dataset
        :type range_col_name: string
        :return: new dataset with two columns mapping each subject URI to the
            object connected by the passed predicate
        :rtype: Dataset
        """
        return ExpandableDataset(self, new_dataset_name, domain_col_name, domain_col_name) \
            .expand(domain_col_name, [(feature, range_col_name, False, PredicateDirection.OUTGOING)])

    def dataset_with_entities(self, entities, new_dataset_name='dataset', entities_col_name='entities'):
        """
        Creates a new one-column dataset filled with the passed entities
        :param entities: list of entities URIs
        :type entities: list of strings
        :param new_dataset_name: the name of the created dataset holding the entities
        :type new_dataset_name: string
        :param entities_col_name: the entities column name in the created dataset
        :type entities_col_name: string
        :return: dataset with one column filled with the passed entities URIs
        :rtype: Dataset
        """
        return ExpandableDataset(self, new_dataset_name, entities, entities_col_name)

    def describe_entity(self, entity, new_dataset_name='dataset', class_col_name='class', feature_col_name='feature'):
        """
        Retrieves the class and the features of a specific entity
        Equivalent to the query:
        select ?class ?p
        where {
            ?e  type ?class
            ?e ?p ?o
        }        
        :param entity: entity uri
        :type entity: string
        :param class_col_name: the class column name in the returned dataset
        :type class_col_name: string
        :param new_dataset_name: the name of the created dataset holding the entities
        :type new_dataset_name: string
        :return: (class, list of features)
        :rtype: tuple of (string, list of strings)
        """
        return ExpandableDataset(self, new_dataset_name, entity, 'instance')\
            .expand('instance', [('rdf:type', class_col_name, False, PredicateDirection.OUTGOING)])\
            .expand('instance', [(feature_col_name, "feature_value", False, PredicateDirection.OUTGOING)])

