from rdfframes.query_buffer.query_operators.query_queue_operator import QueryQueueOperator
from rdfframes.utils.constants import AggregationFunction

__author__ = """
Abdurrahman Ghanem <abghanem@hbku.edu.qa>
Aisha Mohamed <ahmohamed@qf.org.qa>
"""


class IntegerCountOperator(QueryQueueOperator):
    def __init__(self, src_dataset_name, agg_tag=None, agg_param=None):
        """
        Represents an aggregation function on a normal dataset like SUM, AVG ... etc
        :param src_dataset_name: the dataset name where the source column resides
        :param agg_tag: aggregation function alias
        :param agg_param: aggregation parameter like distinct with count
        """
        super(IntegerCountOperator, self).__init__(src_dataset_name)
        self.function = AggregationFunction.COUNT
        self.agg_tag = agg_tag if agg_tag is not None else '{}_{}'.format(src_col_name, agg_fn)
        self.agg_parameter = agg_param
        self._id = self.create_id()

    def create_id(self):
        return '{}.all'.format(self.function)

    def parent_ids(self):
        raise Exception("COUNT ALL column has not parent")

    def node_adds_col(self):
        return False

    def operation_name(self):
        return self.function

    def visit_node(self, query_model, ds, parent):
        query_model.add_aggregate_pair("*", self.function, self.agg_tag, self.agg_parameter)
        query_model.auto_add_select_column(self.agg_tag)
        return ds, query_model, None

    def __repr__(self):
        return '''Aggregation Node:
            src_ds: {}
            src_col: all
            agg_function: {}
            agg_parameters: {}
            agg_tag: {}'''.format(self.src_dataset_name, self.function, self.agg_parameter,
                                  self.agg_tag)
