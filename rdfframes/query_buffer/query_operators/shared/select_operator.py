from rdfframes.query_buffer.query_operators.query_queue_operator import QueryQueueOperator

__author__ = """
Abdurrahman Ghanem <abghanem@hbku.edu.qa>
Aisha Mohamed <ahmohamed@qf.org.qa>
Ghadeer Abuoda <gabuoda@hbku.edu.qa>
"""


class SelectOperator(QueryQueueOperator):
    def __init__(self, src_dataset_name, selected_cols):
        """
        Represents a selection clause in the SPARQL query
        :param src_dataset_name: Which dataset this node belongs to
        :param selected_cols: the column to retrieve its data in the SPARQL query
        """
        super(SelectOperator, self).__init__(src_dataset_name)
        self.selected_cols = selected_cols
        self._id = self.create_id()

    def create_id(self):
        return '{}.select.{}'.format(self.src_dataset_name, sorted(self.selected_cols))

    def parent_ids(self):
        return ['{}.{}'.format(self.src_dataset_name, col) for col in self.selected_cols]

    def node_adds_col(self):
        return False

    def operation_name(self):
        return 'Select'

    def visit_node(self, query_model, ds, parent):
        return_query_model = query_model
        """
        if self.requires_nested_query(ds):  # there is a select column that is not in the inner query
            parent_ds_cols = [col for col in self.selected_cols if col not in ds.columns]
            if len(query_model.subqueries) > 0:
                query_model.transfer_select_triples_to_parent_query(parent_ds_cols)
                return_query_model = query_model
            else:
                return_query_model = query_model.wrap_in_a_parent_query()
                return_query_model.transfer_select_triples_to_parent_query(parent_ds_cols)
        """
        for col in self.selected_cols:
            return_query_model.add_select_column(col)

        return ds, return_query_model, None

    def requires_nested_query(self, ds):
        if ds.type() == "ExpandableDataset":
            return False
        else:  # grouped dataset:
            # check if the select columns are not all in
            # (group_by columns, aggregation columns, columns created after groupby)
            # assert ds.columns == group_by columns, aggregation columns, columns created after groupby
            for col in self.selected_cols:
                if col not in ds.columns:
                    return True

    def __repr__(self):
        return "Dataset_name {}, Selected Columns {}".format(self.src_dataset_name, sorted(self.selected_cols))
