import copy

from rdfframes.query_buffer.query_operators.query_queue_operator import QueryQueueOperator
from rdfframes.query_builder.queue2querymodel import Queue2QueryModelConverter
from rdfframes.utils.constants import JoinType
from rdfframes.query_builder.querymodel import QueryModel


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

        # join the two query models
        joined_query_model = self.__join(query_model, ds2_query_model)

        return ds, joined_query_model, None

    def __join(self, query_model1, query_model2):
        # rename all variables of the first query model if necessary
        if self.src_col_name != self.new_col_name:
            query_model1.rename_variable(self.src_col_name, self.new_col_name)

        # rename all variables of the second query model if necessary
        if self.second_col_name != self.new_col_name:
            query_model2.rename_variable(self.second_col_name, self.new_col_name)

        if query_model1.from_clause != query_model2.from_clause:
            raise Exception("Join on two different graphs is not implemented")
        # union the prefixes
        query_model1.add_prefixes(query_model2.prefixes)

        query_model1.set_offset(min(query_model1.offset, query_model2.offset))
        query_model1.set_limit(max(query_model1.limit, query_model2.limit))
        query_model1.add_order_columns(query_model2.order_clause)

        # TODO: WHY do this here?
        # union the select columns
        if len(query_model1.select_columns) > 0 and len(query_model1.select_columns) > 0:
            query_model1.select_columns = query_model1.select_columns.union(query_model2.select_columns)
        elif len(query_model1.select_columns) > 0:
            query_model1.select_columns = query_model1.select_columns.union(query_model2.variables)
        elif len(query_model2.select_columns) > 0:
            query_model1.select_columns = query_model1.variables.union(query_model2.select_columns)
        query_model1.variables = query_model1.variables.union(query_model2.variables)

        if self.dataset.type() == "ExpandableDataset":
            if self.second_dataset.type() == "ExpandableDataset":
                query_model = self.__join_expandable_expandable(query_model1, query_model2)
            else:  # ds2 is grouped while ds1 is expandable
                query_model = self.__join_expandable_grouped(query_model1, query_model2, expandable_order=1)
        else:  # ds1 is grouped
            if self.second_dataset.type() == "ExpandableDataset":  # ds2 is expandable while ds1 is grouped
                query_model = self.__join_expandable_grouped(query_model1, query_model2, expandable_order=2)
            else:  # ds2 is grouped
                query_model = self.__join_grouped_grouped(query_model1, query_model2)
        return query_model

    def __join_expandable_expandable(self, query_model1, query_model2):
        if len(query_model1.aggregate_clause) > 0 or len(query_model2.aggregate_clause) > 0:
            raise Exception("Can't join flat aggregated datasets")
        # TODO: Union the graphs in ds2 with ds1 and assign each graph pattern to a graph in
        #  case of joining two different graphs
        if self.join_type == JoinType.InnerJoin:
            # add the basic graph patterns in dataset2 to dataset1
            for triple in query_model2.triples:
                query_model1.add_triple(*triple)
            # append the optional patterns in dataset2 to optionals in dataset1
            for optional_block in query_model2.optionals:
                query_model1.add_optional_block(optional_block)
            # add the filter graph patterns of dataset2 to dataset1
            for column, conditions in query_model2.filter_clause.items():
                for condition in conditions:
                    query_model1.add_filter_condition(column, condition)
            for query in query_model2.subqueries:
                query_model1.add_subquery(query)
            for query in query_model2.optional_subqueries:
                query_model1.add_optional_subquery(query)
            for query in query_model2.unions:
                query_model1.add_unions(query)
            return query_model1

        elif self.join_type == JoinType.LeftOuterJoin:
            # add the basic and optionals graph patterns of dataset2 to dataset1 in one optional block
            optional_query_model = query_model1.add_optional_triples(query_model2.triples)
            for optioanl_block in query_model2.optionals:
                optional_query_model.add_optional_block(optioanl_block)
            for column, conditions in query_model2.filter_clause.items():
                for condition in conditions:
                    optional_query_model.add_filter_condition(column, condition)
            for query in query_model2.subqueries:
                optional_query_model.add_subquery(query)
            for query in query_model2.optional_subqueries:
                optional_query_model.add_optional_subquery(query)
            for query in query_model2.unions:
                optional_query_model.add_unions(query)
            return query_model1

        elif self.join_type == JoinType.RightOuterJoin:
            # move all triples, filters and optionals of dataset1 to one optional block in dataset 1.
            query_model1_optionals = copy.copy(query_model1.optionals)
            optional_query_model = query_model1.add_optional_triples(query_model1.triples)
            for column, conditions in query_model1.filter_clause.items():
                for condition in conditions:
                    optional_query_model.add_filter_condition(column, condition)
            for optioanl_block in query_model1_optionals:
                optional_query_model.add_optional_block(optioanl_block)
            for query in query_model1.subqueries:
                optional_query_model.add_subquery(query)
            for query in query_model1.optional_subqueries:
                optional_query_model.add_optional_subquery(query)
            for query in query_model1.unions:
                optional_query_model.add_unions(query)
            # remove the patterns that moved to the optional block from the original query model
            query_model1.rem_all_triples()
            query_model1.rem_optional_triples()
            query_model1.rem_filters()
            query_model1.rem_subqueries()
            query_model1.rem_optional_subqueries()
            query_model1.rem_unions()
            # add the optional block to dataset 1
            query_model1.add_optional_block(optional_query_model)
            # add the triples in dataset2 to triples in the original query model of dataset 1
            for triple in query_model2.triples:
                query_model1.add_triple(*triple)
            for optioanl_block in query_model2.optionals:
                query_model1.add_optional_block(optioanl_block)
            for column, conditions in query_model2.filter_clause.items():
                for condition in conditions:
                    query_model1.add_filter_condition(column, condition)
            for query in query_model2.subqueries:
                query_model1.add_subquery(query)
            for query in query_model2.optional_subqueries:
                query_model1.add_optional_subquery(query)
            for query in query_model2.unions:
                query_model1.add_unions(query)
            return query_model1
        else:  # outer join
            return JoinOperator._outer_join(query_model1, query_model2)

    # first query_model1 comes always from an expandable dataset and query_model2 from a grouped dataset
    def __join_expandable_grouped(self, query_model1, query_model2, expandable_order=1):
        if self.join_type == JoinType.OuterJoin:  # outer join
            return JoinOperator._outer_join(query_model1, query_model2)
        elif self.join_type == JoinType.InnerJoin :
                # add query model 2 as a subquery
                QueryModel.clean_inner_qm(query_model2)
                query_model1.add_subquery(query_model2)
                return query_model1
        elif ((expandable_order == 1 and self.join_type == JoinType.LeftOuterJoin) or\
            (expandable_order == 2 and self.join_type == JoinType.RightOuterJoin)):
            # make the subquery optional
            QueryModel.clean_inner_qm(query_model2)
            query_model1.add_optional_subquery(query_model2)
            return query_model1
        elif ((expandable_order == 2 and self.join_type == JoinType.LeftOuterJoin) or\
            (expandable_order == 1 and self.join_type == JoinType.RightOuterJoin)):
            # create an outer query and add the main dataset as a subquery and the optional dataset as optional subquery
            joined_query_model = QueryModel()
            joined_query_model.prefixes = copy.copy(query_model1.prefixes)  # all prefixes are already in query_model1
            joined_query_model.variables = copy.copy(query_model1.variables)  # all prefixes are already in query_model1
            joined_query_model.from_clause = copy.copy(query_model1.from_clause)
            joined_query_model.select_columns = copy.copy(query_model1.select_columns)
            joined_query_model.offset = query_model1.offset
            joined_query_model.limit = query_model1.limit
            joined_query_model.order_clause = copy.copy(query_model1.order_clause)
            QueryModel.clean_inner_qm(query_model1)
            QueryModel.clean_inner_qm(query_model2)
            joined_query_model.add_optional_subquery(query_model1)
            joined_query_model.add_subquery(query_model2)
            return joined_query_model
        else:
            raise Exception("Undefined case")

    def __join_grouped_grouped(self, query_model1, query_model2):
        joined_query_model = QueryModel()

        joined_query_model.prefixes = copy.copy(query_model1.prefixes)# all prefixes are already in query_model1
        joined_query_model.add_prefixes(query_model2.prefixes)

        joined_query_model.variables = copy.copy(query_model1.variables.union(query_model2.variables))  # all prefixes are already in query_model1
        joined_query_model.from_clause = copy.copy(query_model1.from_clause)
        joined_query_model.select_columns = copy.copy(query_model1.select_columns.union(query_model2.select_columns))
        joined_query_model.offset = min(query_model1.offset, query_model2.offset)
        joined_query_model.limit = max(query_model1.limit, query_model2.limit)
        query_model1.order_clause.update(query_model2.order_clause)
        joined_query_model.order_clause = copy.copy(query_model1.order_clause)

        QueryModel.clean_inner_qm(query_model1)
        QueryModel.clean_inner_qm(query_model2)

        # add subqueries
        if self.join_type == JoinType.InnerJoin:
            joined_query_model.add_subquery(query_model1)
            joined_query_model.add_subquery(query_model2)
        elif self.join_type == JoinType.LeftOuterJoin:
            joined_query_model.add_subquery(query_model1)
            joined_query_model.add_optional_subquery(query_model2)
        elif self.join_type == JoinType.RightOuterJoin:
            joined_query_model.add_subquery(query_model2)
            joined_query_model.add_optional_subquery(query_model1)
        else:  # outer join
            joined_query_model.add_unions(query_model1)
            joined_query_model.add_unions(query_model2)
        return joined_query_model

    def __repr__(self):
        """
        return: node to string representation
        """
        return "Join Operator dataset1: {}  dataset2: {} col1: {} col2: {} join type: {}".format(
            self.src_datset_name, self.second_dataset.name, self.src_col_name, self.second_col_name, self.join_type)

    @staticmethod
    def _outer_join(query_model1, query_model2):
        joined_query_model = QueryModel()
        joined_query_model.prefixes = copy.copy(query_model1.prefixes)  # all prefixes are already in query_model1
        joined_query_model.variables = copy.copy(query_model1.variables)  # all prefixes are already in query_model1
        joined_query_model.from_clause = copy.copy(query_model1.from_clause)
        joined_query_model.select_columns = copy.copy(query_model1.select_columns)
        joined_query_model.offset = query_model1.offset
        joined_query_model.limit = query_model1.limit
        joined_query_model.order_clause = copy.copy(query_model1.order_clause)
        QueryModel.clean_inner_qm(query_model1)
        QueryModel.clean_inner_qm(query_model2)
        joined_query_model.add_unions(query_model1)
        joined_query_model.add_unions(query_model2)
        return joined_query_model