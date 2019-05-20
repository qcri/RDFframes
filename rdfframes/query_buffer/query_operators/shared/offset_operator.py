from rdfframes.query_buffer.query_operators.query_queue_operator import QueryQueueOperator

__author__ = """
Abdurrahman Ghanem <abghanem@hbku.edu.qa>
Aisha Mohamed <ahmohamed@qf.org.qa>
Ghadeer Abuoda <gabuoda@hbku.edu.qa>
"""


class OffsetOperator(QueryQueueOperator):
    """
    Represents an offset node in the query tree
    """
    def __init__(self, src_dataset_name, offset):
        """
        Initializes the node with the information about the created offset step
        :param src_dataset_name:
        :param offset:
        """
        super(OffsetOperator, self).__init__(src_dataset_name=src_dataset_name)
        self.offset = offset
        self._id = self.create_id()

    def create_id(self):
        """
        :return: a string unique identifier of the node
        """
        return '{}.offset.{}'.format(super(OffsetOperator, self).id(), self.offset)

    def node_adds_col(self):
        return False

    def operation_name(self):
        return 'Offset'

    def visit_node(self, query_model, ds, parent):
        query_model.set_offset(self.offset)
        return ds, query_model, None

    def __repr__(self):
        return 'dataset: {}, offset: {}'.format(self.src_dataset_name, self.offset)
