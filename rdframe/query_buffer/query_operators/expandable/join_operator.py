from orderedset._orderedset import OrderedSet

from rdframe.query_buffer.query_operators.query_queue_operator import QueryQueueOperator
from rdframe.query_builder.queue2querymodel import Queue2QueryModelConverter
from rdframe.utils.constants import JoinType
from rdframe.query_builder.querymodel import QueryModel
__author__ = """
Ghadeer Abuoda <gabuoda@hbku.edu.qa>
Aisha Mohamed <ahmohamed@qf.org.qa>
Zoi Kaoudi <zkaoudi@hbku.edu.qa>
"""


class JoinOperator(QueryQueueOperator):
    """
    Represents a join step in the query tree
    """

    def __init__(self, dataset1, dataset2, join_col_name1, join_col_name2, join_type, new_column_name=None):
        """
        Initializes the node with the information about the created join step
        :param first_dataset_name: the dataset name where the source column resides
        :param first_col_name: the column name that the join operation will operate on in the source dataset
        :param second_dataset_name: the dataset name that will be joined with first_dataset_name
        :param second_col_name: the column name that the join operation will operate on in the second dataset
        :param join_type: one of [inner, left_orter, right_outer]
        """
        super(JoinOperator, self).__init__(src_dataset_name=dataset1.name)
        self.dataset = dataset1
        self.second_dataset = dataset2
        self.src_col_name = join_col_name1
        self.second_col_name = join_col_name2
        self.join_type = join_type
        self.new_col_name = new_column_name
        self._id = self.create_id()

    def create_id(self):
        return '{}.{}.join.{}.{}'.format(self.dataset.name, self.src_col_name,
                                         self.second_dataset.name, self.second_col_name)

    def node_adds_col(self):
        return self.new_col_name is not None or len(self.second_dataset.columns) > 1

    def operation_name(self):
        return 'Join'

    def visit_node(self, query_model, ds, parent):
        # evaluate the query model of dataset2
        converter = Queue2QueryModelConverter(self.second_dataset)
        ds2_query_model = converter.to_query_model()

        # rename all variables of the first query model if necessary
        # FIXME: if there is a select node after the join it may contain a variable name that does not exist anymore..
        if self.src_col_name != self.new_col_name:
            query_model.rename_variable(self.src_col_name, self.new_col_name)

        # rename all variables of the second query model if necessary
       # if self.second_col_name != self.new_col_name:
        #    ds2_query_model.rename_variable(self.second_col_name, self.new_col_name)

        # union the prefixes
        prefixes2 = {}
        for prefix in ds2_query_model.prefixes:
            if prefix not in query_model.prefixes:
                prefixes2[prefix] = ds2_query_model.prefixes[prefix]
        query_model.add_prefixes(prefixes2)

        if self.second_dataset.type() == "ExpandableDataset":
            # TODO: Union the graphs in ds2 with ds1 and assign each graph pattern to a graph in case of joining two different graphs
            # union the select columns
            query_model.select_columns = query_model.select_columns.union(
                ds2_query_model.select_columns)  # FIXME: there will be no select_cols in the query model because we always process the select node in the end

            # union the variables
            query_model.variables = query_model.variables.union(ds2_query_model.variables)

            query_model.set_offset(min(query_model.offset, ds2_query_model.offset))
            query_model.set_limit(max(query_model.limit, ds2_query_model.limit))
            query_model.add_order_columns(ds2_query_model.order_clause)

            # add the filter graph patterns in dataset2 to dataset1
            for column, condition in ds2_query_model.filter_clause:
                query_model.add_filter_condition(column, condition)

            if self.join_type == JoinType.InnerJoin:
                # add the basic graph patterns in dataset2 to dataset1
                for triple in ds2_query_model.triples:
                    query_model.add_triple(*triple)
                # append the optional patterns in dataset2 to optionals in dataset1
                for op_triple in ds2_query_model.optionals:
                    query_model.add_optional(*op_triple)
            elif self.join_type == JoinType.LeftOuterJoin:
                # add the basic and optionals graph patterns of dataset2 to dataset1 optionals
                for triple in ds2_query_model.triples:
                    query_model.add_optional(*triple)
                ## TODO: change the structure of the optional block; the optional in the original query in first block then the optional block from the second query model
                for triple in ds2_query_model.optionals:
                    query_model.add_optional(*triple)
            elif self.join_type == JoinType.RightOuterJoin:
                # move all triples of dataset1 to optional
                for triple in query_model.triples:
                    query_model.add_optional(*triple)
                query_model.rem_all_triples()
                # add the triples in dataset2 to triples in dataset1
                for triple in ds2_query_model.triples:
                    query_model.add_triple(*triple)
                # append the optional patterns in dataset2 to optionals in dataset1
                for op_triple in ds2_query_model.optionals:
                    query_model.add_optional(*op_triple)
            elif  self.join_type == JoinType.OuterJoin:
               # The join will build three queries and two sub-queries
                # one contains the basic pattrens from the two dataset into one query
                ds1_query_model = QueryModel()
                inner_query_model=  QueryModel()

                for triple in query_model.triples:
                   ds1_query_model.add_triple(*triple)
                   inner_query_model.add_triple(*triple)
                for op_triple in query_model.optionals:
                    ds1_query_model.add_optional(*op_triple)
                    inner_query_model.add_optional(*op_triple)

                query_model.rem_all_triples()
                query_model.rem_optionaltriples()

                for triple in ds2_query_model.triples:
                    inner_query_model.add_triple(*triple)

                for op_triple in ds2_query_model.optionals:
                    inner_query_model.add_optional(*op_triple)

            ## one query with basic pattren from dataset1 and basic pattren from dataset2 as optional

                ds1_triples = ds1_query_model.triples.copy()
                ds1_optional = ds1_query_model.optionals.copy()
                ds2_triples = ds2_query_model.triples.copy()
                ds2_optional = ds2_query_model.optionals.copy()

                for triple in ds2_triples:
                    ds1_query_model.add_optional(*triple)
                for op_triple in ds2_optional:
                    ds1_query_model.add_optional(*op_triple)

            ## one query with basic pattren from dataset 2 and basic pattren from dataset1 as optional
                for triple in ds1_triples:
                    ds2_query_model.add_optional(*triple)
                for op_triple in ds1_optional:
                    ds2_query_model.add_optional(*op_triple)

            # add the two queries into the union of the main query
                query_model.select_columns = query_model.select_columns.union(ds2_query_model.select_columns)

                query_model.add_unions(ds1_query_model)
                #ds2_query_model.rem_prefixes()
                query_model.add_unions(ds2_query_model)
                query_model.add_unions(inner_query_model)
        else:
            if self.join_type == JoinType.InnerJoin:
                # add query model 2 as a subquery
                query_model.add_subquery(ds2_query_model)
            if self.join_type == JoinType.LeftOuterJoin:
                # make the subquery optional
                query_model.add_optional_subquery(ds2_query_model)
            if self.join_type == JoinType.RightOuterJoin:
                # move all triples of dataset1 to optional
                for triple in query_model.triples:
                    query_model.add_optional(*triple)
                query_model.rem_all_triples()
                # add query model 2 as a subquery
                query_model.add_subquery(ds2_query_model)



        return ds, query_model, None

    def __repr__(self):
        """
        return: node to string representation
        """
        return "Join Operator dataset1: {}  dataset2: {} col1: {} col2: {} join type: {}".format(
            self.src_datset_name, self.second_dataset.name, self.src_col_name, self.second_col_name, self.join_type)
