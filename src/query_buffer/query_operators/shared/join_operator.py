from src.query_buffer.query_operators.query_queue_operator import QueryQueueOperator

__author__ = """
Abdurrahman Ghanem <abghanem@hbku.edu.qa>
Aisha Mohamed <ahmohamed@qf.org.qa>
"""


class JoinOperator(QueryQueueOperator):
    """
    Represents a join step in the query tree
    """
    def __init__(self, first_dataset_name, first_col_name, second_dataset_name, second_col_name, join_type):
        """
        Initializes the node with the information about the created join step
        :param first_dataset_name: the dataset name where the source column resides
        :param first_col_name: the column name that the join operation will operate on in the source dataset
        :param second_dataset_name: the dataset name that will be joined with first_dataset_name
        :param second_col_name: the column name that the join operation will operate on in the second dataset
        :param join_type: one of [inner, left_orter, right_outer]
        """
        super(JoinOperator, self).__init__(src_dataset_name=first_dataset_name)
        self.src_col_name = first_col_name
        self.second_dataset_name = second_dataset_name
        self.second_col_name = second_col_name
        self.join_type = join_type
        self._id = self.create_id()

    def create_id(self):
        return '{}.{}.join.{}.{}'.format(self.src_dataset_name, self.src_col_name,
                                         self.second_dataset_name, self.second_dataset_name)

    def node_adds_col(self):
        return True

    def operation_name(self):
        return 'Join'

    def visit_node(self, query_model, ds, parent):
        return super(JoinOperator, self).visit_node(query_model, ds, parent)

    def __repr__(self):
        """
        return: node to string representation
        """
        return "Join Operator dataset1: {}  dataset2: {} col1: {} col2: {} join type: {}".format(
            self.src_datset_name, self.second_dataset_name,  self.src_col_name, self.second_col_name, self.join_type)


