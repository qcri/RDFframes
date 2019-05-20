from rdfframes.query_buffer.query_operators.query_queue_operator import QueryQueueOperator

__author__ = """
Abdurrahman Ghanem <abghanem@hbku.edu.qa>
Aisha Mohamed <ahmohamed@qf.org.qa>
"""


class GroupBySeedOperator(QueryQueueOperator):
    def __init__(self, grouped_dataset_name, groupby_node):
        super(GroupBySeedOperator, self).__init__(grouped_dataset_name)
        self.src_col_name = '{}.seed'.format(grouped_dataset_name)
        self.groupby_node = groupby_node
        self._id = self.create_id()

    def create_id(self):
        return '{}.seed'.format(self.src_dataset_name)

    def parent_ids(self):
        raise Exception('Seed node does not have parent')

    def node_adds_col(self):
        return True

    def visit_node(self, query_model, ds, parent):
        return ds, query_model, None

    def __repr__(self):
        return "Grouped_seed {}, group_by node = {}".format(super(GroupBySeedOperator, self).__repr__(), self.groupby_node)
