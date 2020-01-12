from rdfframes.query_buffer.query_queue import QueryQueue
from rdfframes.query_buffer.query_operators.query_queue_operator import QueryQueueOperator


__author__ = """
Abdurrahman Ghanem <abghanem@hbku.edu.qa>
Aisha Mohamed <ahmohamed@qf.org.qa>
"""


class AggregationOperator(QueryQueueOperator):
    def __init__(self, src_dataset_name, src_col_name, agg_fn, agg_tag=None, agg_param=None):
        """
        Represents an aggregation function on a normal dataset like SUM, AVG ... etc
        :param src_dataset_name: the dataset name where the source column resides
        :param src_col_name: the column name that will be expanded in the source dataset
        :param agg_tag: aggregation function alias
        :param agg_param: aggregation parameter like distinct with count
        :param agg_fn: the name of the aggregation function as string
        """
        super(AggregationOperator, self).__init__(src_dataset_name)
        self.src_col_name = src_col_name
        self.function = agg_fn
        self.agg_tag = agg_tag if agg_tag is not None else '{}_{}'.format(src_col_name, agg_fn)
        self.agg_parameter = agg_param
        self._id = self.create_id()

    def create_id(self):
        return '{}.{}'.format(QueryQueue.create_node_id(self.src_dataset_name, self.src_col_name), self.function)

    def parent_ids(self):
        parent_id = '{}.{}'.format(self.src_dataset_name, self.src_col_name)
        return [parent_id]

    def node_adds_col(self):
        return False

    def operation_name(self):
        return self.function

    def visit_node(self, query_model, ds, parent):
        if self.src_col_name in ds.agg_columns:
            if self.not_already_in_outer_query(ds, query_model):
                query_model = query_model.wrap_in_a_parent_query()
        query_model.add_aggregate_pair(self.src_col_name, self.function, self.agg_tag, self.agg_parameter)
        query_model.auto_add_select_column(self.agg_tag)
        return ds, query_model, None

    def not_already_in_outer_query(self, ds, query_model):
        return ds.type() == "GroupedDataset" and len(query_model.groupBy_columns) > 0

    def __repr__(self):
        return '''Aggregation Node:
            src_ds: {}
            src_col: {}
            agg_function: {}
            agg_parameters: {}
            agg_tag: {}'''.format(self.src_dataset_name, self.src_col_name, self.function, self.agg_parameter,
                                  self.agg_tag)
