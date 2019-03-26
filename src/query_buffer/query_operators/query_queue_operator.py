__author__ = """
Abdurrahman Ghanem <abghanem@hbku.edu.qa>
Aisha Mohamed <ahmohamed@qf.org.qa>
"""


class QueryQueueOperator:
    """
    Represents an expansion step of a dataset. A node is added to the query_buffer in order to preserve the execution
    order of expansion call to be transformed later to SPARQL query
    Subclasses should
    - implement create_id()
    - define self._id = self.create_id() in the initialization
    """

    def __init__(self, src_dataset_name):
        """
        Initializes the node with the information about the created step
        :param src_dataset_name: the dataset name where the source column resides
        """
        self.src_dataset_name = src_dataset_name
        self._id = ''

    def create_id(self):
        pass

    def parent_ids(self):
        pass

    def id(self):
        """
        :return: a string unique identifier of the node
        """
        return self._id

    def node_adds_col(self):
        pass

    def operation_name(self):
        pass

    def visit_node(self, query_model, ds, parent):
        return ds, query_model, None

    def __repr__(self):
        """
        :return: node to string representation
        """
        pass

    def __str__(self):
        return self._id

    def __hash__(self):
        return hash(self.id())

    def __eq__(self, other):
        return self.id() == other.id()
