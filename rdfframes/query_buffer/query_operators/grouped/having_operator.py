from rdfframes.query_buffer.query_operators.query_queue_operator import QueryQueueOperator
from rdfframes.query_buffer.query_operators.grouped.grouped_aggregation_operator import GroupedAggregationOperator

__author__ = """
Abdurrahman Ghanem <abghanem@hbku.edu.qa>
Aisha Mohamed <ahmohamed@qf.org.qa>
Ghadeer Abuoda <gabuoda@hbku.edu.qa>
"""


class HavingOperator(QueryQueueOperator):
    """
    Represents a filter step on the result of an aggregation node in the query graph
    """
    def __init__(self, src_dataset_name, src_col_name, conditions):
        """
        Initializes the node with the information about the created filter step
        :param src_dataset_name: the dataset name where the source column resides
        :param src_col_name: the column name that will be filtered in the source dataset. It has to be a column that was
            created by an aggregate function. to filter on non aggregate function result, create a filter node.
        :param conditions: the filter condition as string or a list of strings
        """
        super(HavingOperator, self).__init__(src_dataset_name=src_dataset_name)
        self.src_col_name = src_col_name
        self.conditions = conditions
        self._id = self.create_id()

    def create_id(self):
        """
        :return: a string unique identifier of the node
        """
        return '{}.{}.having.{}'.format(self.src_dataset_name, self.src_col_name, self.conditions)

    def parent_ids(self):
        parent_id = '{}.{}'.format(self.src_dataset_name, self.src_col_name)
        return [parent_id]

    def node_adds_col(self):
        return False

    def operation_name(self):
        return 'Filter'

    def visit_node(self, query_model, ds, parent):
        nodes = ds.query_queue.get_nodes_of_type(GroupedAggregationOperator)

        for node in nodes:
            if node.new_col_name == self.src_col_name:
                for condition in self.conditions:
                    query_model.add_having_condition(self.src_col_name, condition)

        return ds, query_model, None

    def __repr__(self):
        return "Having Node: src_ds:{}, src_col: {}, conditions: {}".format(
            self.src_dataset_name, self.src_col_name, self.conditions)

