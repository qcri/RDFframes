from rdfframes.query_buffer.query_operators.query_queue_operator import QueryQueueOperator

__author__ = """
Abdurrahman Ghanem <abghanem@hbku.edu.qa>
Aisha Mohamed <ahmohamed@qf.org.qa>
Ghadeer Abuoda <gabuoda@hbku.edu.qa>
Zoi Kaoudi <zkaoudi@hbku.edu.qa
"""


class SortOperator(QueryQueueOperator):
    """
    Represents an order by step in the query tree
    """

    def __init__(self, src_dataset_name, sorting_cols):
        """
        Initializes the node with the information about the created sort step
        :param src_dataset_name: the dataset name where the source column resides.
        :param sorting_cols: list of tuples (column name, sort order) that needs to be sorted in the source dataset.
        """
        super(SortOperator, self).__init__(src_dataset_name=src_dataset_name)
        self.sorting_cols = sorting_cols
        self._id = self.create_id()

    def create_id(self):
        """
        :return: a string unique identifier of the node
        """
        return '{}.sort.{}'.format(self.src_dataset_name, self.sorting_cols)

    def node_adds_col(self):
        return False

    def operation_name(self):
        return 'Sort'

    def visit_node(self, query_model, ds, parent):
        target_qm = query_model

        while target_qm.parent_query_model is not None:
            target_qm = target_qm.parent_query_model

        target_qm.add_order_columns(self.sorting_cols)
        return ds, query_model, None

    def __repr__(self):
        return "Sort Node: dataset: {} sort_columns: {}".format(self.src_dataset_name, self.sorting_cols)
