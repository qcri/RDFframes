from rdfframes.query_buffer.query_operators.query_queue_operator import QueryQueueOperator

__author__ = """
Abdurrahman Ghanem <abghanem@hbku.edu.qa>
Aisha Mohamed <ahmohamed@qf.org.qa>
Ghadeer Abuoda <gabuoda@hbku.edu.qa>
"""


class LimitOperator(QueryQueueOperator):
    """
    Represents a threshold node in the query tree
    """
    def __init__(self, src_dataset_name, threshold):
        """
        Initializes the node with the information about the created limit step
        :param src_dataset_name:
        :param threshold:
        """
        super(LimitOperator, self).__init__(src_dataset_name=src_dataset_name)
        self.threshold = threshold
        self._id = self.create_id()

    def create_id(self):
        """
        :return: a string unique identifier of the node
        """
        return '{}.limit.{}'.format(self.src_dataset_name, self.threshold)

    def node_adds_col(self):
        return False

    def operation_name(self):
        return 'Limit'

    def visit_node(self, query_model, ds, parent):
        query_model.set_limit(self.threshold)
        return ds, query_model, None

    def __repr__(self):
        return 'dataset: {}, limit: {}'.format(self.src_dataset_name, self.threshold)
