from rdfframes.query_buffer.query_operators.query_queue_operator import QueryQueueOperator

__author__ = """
Abdurrahman Ghanem <abghanem@hbku.edu.qa>
Aisha Mohamed <ahmohamed@qf.org.qa>
Zoi Kaoudi <zkaoudi@hbku.edu.qa>
"""


class FilterOperator(QueryQueueOperator):
    """
    Represents a filter step in the query tree
    """
    def __init__(self, src_dataset_name, src_col_name, conditions):
        """
        Initializes the node with the information about the created filter step
        :param src_dataset_name: the dataset name where the source column resides
        :param src_col_name: the column name that will be filtered in the source dataset. It has to be a column that was
            not created by an aggregate function. to filter on aggregate function results, create a having node.
        :param conditions: the filter conditions as string or a list of strings
        """
        super(FilterOperator, self).__init__(src_dataset_name=src_dataset_name)
        self.src_col_name = src_col_name
        self.conditions = conditions
        self._id = self.create_id()

    def create_id(self):
        """
        initializes the node id
        :return:
        """
        return '{}.{}.{}'.format(self.src_dataset_name, self.src_col_name, self.conditions)

    def parent_ids(self):
        parent_id = '{}.{}'.format(self.src_dataset_name, self.src_col_name)
        return [parent_id]

    def node_adds_col(self):
        return False

    def operation_name(self):
        return 'Filter'

    def visit_node(self, query_model, ds, parent):

        # any filter operations on a grouped dataset result in a subquery
        if self.already_in_outer_query(ds, query_model):
            query_model = query_model.wrap_in_a_parent_query()
        query_model.add_filter_condition(self.src_col_name, self.conditions)

        return ds, query_model, None

    def already_in_outer_query(self, ds, query_model):
        return ds.type() == "GroupedDataset" and len(query_model.groupBy_columns) > 0

    def __repr__(self):
        """
        :return: node to string representation
        """
        return """Filter Node:
            src_ds:{}
            src_col: {}
            condition: {}""".format(self.src_dataset_name, self.src_col_name, self.conditions)

