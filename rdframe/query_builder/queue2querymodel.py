from rdframe.query_builder.querymodel import QueryModel
from rdframe.query_buffer.query_operators.shared.select_operator import SelectOperator
from queue import Queue


class Queue2QueryModelConverter(object):
    """
    Converts the query dag to a query model
    """
    def __init__(self, dataset):
        self.dataset = dataset
        self.query_model = QueryModel()
        # add the graph URIs
        if self.dataset.graph.graphs is not None: #self.dataset.graph.graphs
            self.query_model.add_graphs(self.dataset.graph.graphs)
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
        # traverse the query dag
        self.traverse_query()

        return self.query_model

    def traverse_query(self):
        self.__move_select_nodes_to_top()
        self.__process_queue(self.dataset)

    def __remove_select_nodes(self, ds):
        select_nodes = []
        if ds.type() == "GroupedDataset":
            select_nodes.extend(self.__remove_select_nodes(ds.parent_dataset))
        queue = ds.query_queue
        for node in queue.queue:
            if isinstance(node, SelectOperator):
                select_nodes.append(node)
                ds.query_queue.remove_node(node)

        return select_nodes

    def __move_select_nodes_to_top(self):
        select_nodes = self.__remove_select_nodes(self.dataset)
        for node in select_nodes:
            self.dataset.query_queue.append_node(node)

    def __process_queue(self, ds):
        # check if this ds is a grouped_ds, process the parent ds before
        if ds.type() == "GroupedDataset":
            self.__process_queue(ds.parent_dataset)

        queue = ds.query_queue
        current_qm = self.query_model

        for node in queue.queue:
            current_ds, current_qm, grouped_ds = node.visit_node(current_qm, ds, node)

        self.query_model = current_qm

    def __dag_bfs(self, query_model, ds):
        dag = ds.query_dag
        visited_nodes = {node.id(): False for nodes_list in dag.nodes_dict.values() for node in nodes_list}
        nodes_queue = Queue()
        nodes_queue.put(dag.root)
        current_qm = query_model
        current_ds = ds

        while not nodes_queue.empty():
            node = nodes_queue.get()

            for child in dag.get_node_children(node):
                if not visited_nodes[child.id()]:
                    nodes_queue.put(child)

            current_ds, current_qm, grouped_ds = node.visit_node(current_qm, current_ds, node)
            visited_nodes[node.id()] = True

            if grouped_ds is not None:
                self.__dag_bfs(current_qm, grouped_ds)
