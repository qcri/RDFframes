from rdfframes.query_buffer.query_queue import QueryQueue
from rdfframes.query_buffer.query_operators.query_queue_operator import QueryQueueOperator
from rdfframes.dataset.rdfpredicate import PredicateDirection


__author__ = """
Abdurrahman Ghanem <abghanem@hbku.edu.qa>
Aisha Mohamed <ahmohamed@qf.org.qa>
"""


class ExpansionOperator(QueryQueueOperator):
    """
    Represents an expansion step in the query tree
    """

    def __init__(self, dataset_name, src_col_name, predicate, new_col_name, exp_dir, is_optional=False):
        """
        Initializes the node with the information about the created expansion step
        :param dataset_name: the dataset name where the source column resides
        :param src_col_name: the column name that will be expanded in the source dataset
        :param predicate: this expansion step is done based on which RDF predicate on the source column
        :param new_col_name: the name of the created column
        :param exp_dir: whether this expansion step is outgoing or ingoing from/to the rdframe col
        :param is_optional: flag to tell if this expansion step is optional
        """
        super(ExpansionOperator, self).__init__(src_dataset_name=dataset_name)

        self.src_col_name = src_col_name
        self.predicate = predicate
        self.new_col_name = new_col_name
        self.expansion_direction = exp_dir
        self.is_optional = is_optional
        self._id = self.create_id()

    def create_id(self):
        return QueryQueue.create_node_id(self.src_dataset_name, self.new_col_name)

    def parent_ids(self):
        parent_id = '{}.{}'.format(self.src_dataset_name, self.src_col_name)
        return [parent_id]

    def node_adds_col(self):
        return True

    def operation_name(self):
        return 'Expansion'

    def visit_node(self, query_model, ds, parent):
        """
        adds the expansion node to the query model
        :param query_model: the current qquery model
        :param ds: the dataset
        :param parent:
        :return: the dataset, the query model after adding the current node,
        """

        triple = None

        if self.expansion_direction == PredicateDirection.INCOMING:
            triple = (self.new_col_name, self.predicate, self.src_col_name,self.is_optional)
        elif self.expansion_direction == PredicateDirection.OUTGOING:
            triple = (self.src_col_name, self.predicate, self.new_col_name, self.is_optional)

        # any expand operations on a grouped dataset result in a subquery
        if self.not_already_in_outer_query(ds, query_model):
            query_model = query_model.wrap_in_a_parent_query()
            print("After wrapping becaue of the expansion, I should have nothing in the select node of the parent query")
        if triple is not None:
            #print("self.is_optional", self.is_optional)
            if self.is_optional:
                query_model.add_optional_triples([(triple[0],triple[1],triple[2])])
            else:
                query_model.add_triple(triple[0],triple[1],triple[2])
            #vars = [variable for variable in [triple[0], triple[1], triple[2]] if ":" not in variable]
            #if len(vars) > 0:
            #    query_model.transfer_select_triples_to_parent_query(vars)
        else:
            return ds, query_model, None

        return ds, query_model, None

    def not_already_in_outer_query(self, ds, query_model):
        return ds.type() == "GroupedDataset" and len(query_model.groupBy_columns) > 0

    def __repr__(self):
        """
        :return: node to string representation
        """
        return "Expansion_node src_ds: {}, src_col: {}, predicate used: {}, new col name: {}, direction:{}".format(
                    self.src_dataset_name,
                    self.src_col_name,
                    self.predicate,
                    self.new_col_name,
                    self.expansion_direction)
