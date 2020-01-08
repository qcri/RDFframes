from rdfframes.query_builder.querymodel import QueryModel
from rdfframes.query_buffer.query_operators.shared.select_operator import SelectOperator
from queue import Queue


class Queue2QueryModelConverter(object):
    """
    Converts the query buffer to a query model
    """
    def __init__(self, dataset):
        self.dataset = dataset
        self.query_model = QueryModel()
        # add the graph URIs
        if self.dataset.graph.graphs is not None:
            self.query_model.add_graphs(self.dataset.graph.graphs.values())
        else:
            self.query_model.add_graphs([])
        # add the prefixes
        for prefixes in self.dataset.graph.graph_prefixes.values():
            self.query_model.add_prefixes(prefixes)

    def to_query_model(self):
        """
        converts the dataset to a query model
        :return: a query model
        """
        # traverse the query queue
        self.traverse_dataset()
        return self.query_model

    def traverse_dataset(self):
        self.__traverse_dataset(self.dataset)

    def __traverse_dataset(self, ds):
        # check if this ds is a grouped_ds, process the parent ds before
        if ds.type() == "GroupedDataset":
            self.__traverse_dataset(ds.parent_dataset)

        queue = ds.query_queue
        current_qm = self.query_model

        for node in queue.queue:
            current_ds, current_qm, grouped_ds = node.visit_node(current_qm, ds, node)
        self.query_model = current_qm