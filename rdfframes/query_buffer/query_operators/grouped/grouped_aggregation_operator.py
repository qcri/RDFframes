from rdfframes.query_buffer.query_queue import QueryQueue
from rdfframes.query_buffer.query_operators.query_queue_operator import QueryQueueOperator


__author__ = """
Abdurrahman Ghanem <abghanem@hbku.edu.qa>
Aisha Mohamed <ahmohamed@qf.org.qa>
Ghadeer Abuoda <gabuoda@hbku.edu.qa>
"""


class GroupedAggregationOperator(QueryQueueOperator):
    def __init__(self, src_dataset_name, src_col_name, agg_fn, new_col_name, agg_param):
        super(GroupedAggregationOperator, self).__init__(src_dataset_name)
        self.src_col_name = src_col_name
        self.agg_function = agg_fn
        self.new_col_name = new_col_name
        self.agg_parameter = agg_param
        self._id = self.create_id()

    def create_id(self):
        return QueryQueue.create_node_id(self.src_dataset_name, self.new_col_name)

    def parent_ids(self):
        parent_id = '{}.seed'.format(self.src_dataset_name)
        return [parent_id]

    def node_adds_col(self):
        return True

    def operation_name(self):
        return self.agg_function

    def visit_node(self, query_model, ds, parent):
        # if I have no select columns, add aggreagtion colum to the select columns.
        #if len(query_model.select_columns) == 0:
        #    query_model.add_select_column(self.new_col_name)


        query_model.add_aggregate_pair(self.src_col_name, self.agg_function, self.new_col_name, self.agg_parameter)
        query_model.auto_add_select_column(self.new_col_name)
        return ds, query_model, None

    def __repr__(self):
        return "Grouped_Agg {}, agg_fn: {}, new_col_name: {}".format(self._id, self.agg_function, self.new_col_name)
