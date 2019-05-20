from rdfframes.query_buffer.query_queue import QueryQueue
from rdfframes.query_buffer.query_operators.query_queue_operator import QueryQueueOperator

__author__ = """
Abdurrahman Ghanem <abghanem@hbku.edu.qa>
Aisha Mohamed <ahmohamed@qf.org.qa>
"""


class SeedOperator(QueryQueueOperator):
    """
    The root of a query tree. This is the point where all subsequent expansions and transformation are built on
    """
    def __init__(self, dataset_name, uris_list, seed_col_name):
        """
        initializes a SeedNode with a list of uris that could be for classes, entities, predicates ... etc
        :param dataset_name: the name of the containing dataset
        :param uris_list: list of URIs that could be for classes, entities, predicates ... etc, but not literals
        :param seed_col_name: the name of the seed column
        """
        super(SeedOperator, self).__init__(src_dataset_name=dataset_name)
        self.uris = uris_list
        self.seed_col_name = seed_col_name
        self._id = self.create_id()

    def create_id(self):
        return QueryQueue.create_node_id(self.src_dataset_name, self.seed_col_name)

    def parent_ids(self):
        raise Exception('Seed node does not have parent')

    def node_adds_col(self):
        return True

    def visit_node(self, query_model, ds, parent):
        query_model.add_variable(self.seed_col_name)
        return ds, query_model, None

    def __repr__(self):
        return 'seed node: {}'.format(self.src_dataset_name)
