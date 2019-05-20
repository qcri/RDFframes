from rdfframes.utils.helper_functions import vectorize_value


class QueryQueue:
    """
    Queue Datastructure to store the sequence of API calls. Processing this queue produces the SPARQL query
    """

    def __init__(self, dataset):
        """
        Initializing the query dag to belong to a particular dataset. Tree nodes has identifiers and these identifiers
        has to be globally unique
        :param dataset: parent dataset
        """
        self.dataset = dataset
        self.queue = []
        self.nodes_dict = {}

    def append_node(self, node):
        """
        Appending node to one or more parent nodes defined by their col_names.
        :param node: node to append
        :return:
        """
        self.queue.append(node)
        node_id = node.id()
        self.nodes_dict[node_id] = node

    def remove_node(self, node):
        self.queue.remove(node)
        node_id = node.id()
        del self.nodes_dict[node_id]

    def get_node(self, col_name):
        node_id = QueryQueue.create_node_id(self.dataset.name, col_name)
        return self.nodes_dict.get(node_id)

    def get_nodes_of_type(self, node_types):
        node_types = vectorize_value(node_types)
        return [node for node in self.nodes_dict.values() if type(node) in node_types]

    def print_query_queue(self, filename=None):
        out_str = ""
        for node in self.queue:
            out_str += '{}\n'.format(str(node))

        if filename is None:
            print(out_str)
        else:
            with open(filename, 'w') as out_file:
                out_file.write(out_str)

    @staticmethod
    def create_node_id(table_name, col_name):
        if col_name != '':
            return '{}.{}'.format(table_name, col_name)
        return ''

    @staticmethod
    def get_node_id(node):
        return QueryQueue.create_node_id(node.table_name, node.new_col_name)

    @staticmethod
    def node_adds_column(node):
        node_id = node.id()
        return not('.having.' in node_id or
                   '.filter.' in node_id or
                   '.select.' in node_id)
