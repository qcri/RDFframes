from rdframe.query_buffer.query_operators.query_queue_operator import QueryQueueOperator
from rdframe.query_builder.queue2querymodel import Queue2QueryModelConverter
from rdframe.utils.constants import JoinType

__author__ = """
Abdurrahman Ghanem <abghanem@hbku.edu.qa>
Aisha Mohamed <ahmohamed@qf.org.qa>
"""


class JoinOperator(QueryQueueOperator):
    """
    Represents a join step in the query tree
    """
    def __init__(self, dataset_name, dataset2, join_col_name1, join_col_name2, join_type, new_column_name=None):
        """
        Initializes the node with the information about the created join step
        :param first_dataset_name: the dataset name where the source column resides
        :param first_col_name: the column name that the join operation will operate on in the source dataset
        :param second_dataset_name: the dataset name that will be joined with first_dataset_name
        :param second_col_name: the column name that the join operation will operate on in the second dataset
        :param join_type: one of [inner, left_orter, right_outer]
        """
        super(JoinOperator, self).__init__(src_dataset_name=dataset_name)
        self.src_col_name = join_col_name1
        self.second_dataset = dataset2
        self.second_col_name = join_col_name2
        self.join_type = join_type
        self.new_col_name = new_column_name
        self._id = self.create_id()

    def create_id(self):
        return '{}.{}.join.{}.{}'.format(self.src_dataset_name, self.src_col_name,
                                         self.second_dataset.name, self.second_col_name)

    def node_adds_col(self):
        return self.new_col_name is not None or len(self.second_dataset.columns) > 1

    def operation_name(self):
        return 'Join'

    def visit_node(self, query_model, ds, parent):
        if self.join_type == JoinType.InnerJoin:
            # evaluate the query model of dataset2
            converter = Queue2QueryModelConverter(self)
            ds2_query_model = converter.to_query_model()

            # TODO: Union the graphs in ds2 with ds1 and assign each graph pattern to a graph in case of joining two different graphs
            # union the variables, the prefixes and select columns
            query_model.variables = query_model.variables.union(ds2_query_model.variables)
            prefixes2 = {}
            for prefix in ds2_query_model.prefixes:
                if prefix not in query_model.prefixes:
                    prefixes2[prefix] = ds2_query_model.prefixes[prefix]
            query_model.add_prefixes(prefixes2)
            query_model.select_columns = query_model.select_columns.union(ds2_query_model.select_columns)

            query_model.set_offset(min(query_model.offset, ds2_query_model.offset))
            query_model.set_limit(max(query_model.limit, ds2_query_model.limit))
            query_model.add_order_columns(ds2_query_model.order_clause)

            # add the basic graph patterns in dataset2 to dataset1
            for triple in ds2_query_model.triples:
                query_model.add_triple(*triple)
            # append the optional patterns in dataset2 to optionals in dataset1
            for op_triple in ds2_query_model.triples:
                query_model.add_optionals(*op_triple)
            
            # add the filter graph patterns in dataset2 to dataset1

        elif self.join_type == JoinType.LeftOuterJoin:
                # evaluate the query model of dataset2
                converter = Queue2QueryModelConverter(self)
                ds2_query_model = converter.to_query_model()
                # rename the self.second_col_name to self.new_col_name in datase2

                # rename the self.first_col_name to self.new_col_name in datase1

                # TODO: Union the graphs in ds2 with ds1 and assign each graph pattern to a graph
                # union the variables, the prefixes and select columns
                query_model.variables = query_model.variables.union(ds2_query_model.variables)
                prefixes2 = {}
                for prefix in ds2_query_model.prefixes:
                    if prefix not in query_model.prefixes:
                        prefixes2[prefix] = ds2_query_model.prefixes[prefix]
                query_model.add_prefixes(prefixes2)
                query_model.select_columns = query_model.select_columns.union(ds2_query_model.select_columns)

                query_model.set_offset(min(query_model.offset, ds2_query_model.offset))
                query_model.set_limit(max(query_model.limit, ds2_query_model.limit))
                query_model.add_order_columns(ds2_query_model.order_clause)

                # add the basic and optionals graph patterns of dataset2 to dataset1 optionals
                for triple in ds2_query_model.triples:
                    query_model.add_optionals(*triple)
                for triple in ds2_query_model.optionals:
                    query_model.add_optionals(*triple)

                # add the filter graph patterns in dataset2 to dataset1


    def __repr__(self):
        """
        return: node to string representation
        """
        return "Join Operator dataset1: {}  dataset2: {} col1: {} col2: {} join type: {}".format(
            self.src_datset_name, self.second_dataset.name,  self.src_col_name, self.second_col_name, self.join_type)


