from rdfframes.query_buffer.query_operators.query_queue_operator import QueryQueueOperator
from rdfframes.query_buffer.query_operators.grouped.grouped_aggregation_operator import GroupedAggregationOperator
from orderedset import OrderedSet

__author__ = """
Abdurrahman Ghanem <abghanem@hbku.edu.qa>
Aisha Mohamed <ahmohamed@qf.org.qa>
Ghadeer Abuoda <gabuoda@hbku.edu.qa>
"""


class GroupByOperator(QueryQueueOperator):
    """
    holds information about a Group by operation on the dataset
    """
    def __init__(self, src_dataset_name, grouping_cols, new_dataset_name):
        super(GroupByOperator, self).__init__(src_dataset_name)
        self.grouping_cols = OrderedSet(grouping_cols)
        self.new_dataset_name = new_dataset_name
        self.grouped_dataset = None
        self._id = self.create_id()

    def create_id(self):
        return '{}.groupedby.{}'.format(self.src_dataset_name, sorted(self.grouping_cols))

    def parent_ids(self):
        return ['{}.{}'.format(self.src_dataset_name, col) for col in self.grouping_cols]

    def node_adds_col(self):
        return False

    def operation_name(self):
        return 'GroupBy'

    def visit_node(self, query_model, ds, parent):
        return_query_model = query_model

        if self.requires_nested_query(query_model):
            raise Exception("GroupBy operation requires a nested query")
            return_query_model = query_model.wrap_in_a_parent_query()
            # add the group-by operation
            for g_col in self.grouping_cols:
                if g_col not in return_query_model.variables:
                    return_query_model.add_variable(g_col)
                    involved_triples = [triple for triple in query_model.triples
                                        if g_col == triple[0] or g_col == triple[2]]
                    for t in involved_triples:
                        return_query_model.add_triple(*t)
                    # add filter patterns
                    if g_col in query_model.filter_clause:
                        return_query_model.add_filter_condition(g_col, query_model.filter_clause[g_col])
                    # add subqueries
                    for subquery in query_model.subqueries:
                        if g_col in subquery.select_columns:
                            return_query_model.add_subquery(subquery)
                return_query_model.subqueries[len(return_query_model.subqueries) - 1].auto_add_select_column(g_col)
                #return_query_model.subqueries[len(return_query_model.subqueries) - 1].select_all = True
            # add the select columns and group by columns to the inner query
            return_query_model.subqueries[len(return_query_model.subqueries)-1].add_group_columns(self.grouping_cols)
        else:
            # add select and group by columns
            for g_col in self.grouping_cols:
                return_query_model.auto_add_select_column(g_col)
            #return_query_model.select_all = True
            return_query_model.add_group_columns(self.grouping_cols)

        return ds, return_query_model, self.grouped_dataset

    def requires_nested_query(self, query_model):
        # check if the query model has select columns added by the user
        # that are not in the group by columns or aggregation columns
        agg_cols = set([node.new_col_name
                        for node in self.grouped_dataset.query_queue.get_nodes_of_type([GroupedAggregationOperator])])
        return len(query_model.select_columns.difference(agg_cols.union(self.grouping_cols))) > 0

    def __repr__(self):
        return "Group_by_node src_ds:{}, grouping_col: {}, new_dataset_name: {}".format(
            self.src_dataset_name, self.grouping_cols, self.new_dataset_name)
