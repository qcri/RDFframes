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

        if query_model1.from_clause !=  query_model2.from_clause:
            raise Exception("Join on two different graphs is not implemented")
        # union the prefixes
        prefixes2 = {}
        for prefix in query_model2.prefixes:
            if prefix not in query_model1.prefixes:
                prefixes2[prefix] = query_model2.prefixes[prefix]
        query_model1.add_prefixes(prefixes2)

        # union the select columns
        query_model1.select_columns = query_model1.select_columns.union(query_model2.select_columns)

        # union the variables
        query_model1.variables = query_model1.variables.union(query_model2.variables)

        query_model1.set_offset(min(query_model1.offset, query_model2.offset))
        query_model1.set_limit(max(query_model1.limit, query_model2.limit))
        query_model1.add_order_columns(query_model2.order_clause)

        if self.dataset.type() == "ExpandableDataset":
            if self.second_dataset.type() == "ExpandableDataset":
                query_model = self.__join_expandable_expandable(query_model1, query_model2)#, self.join_type)
            else:  # ds2 is grouped while ds1 is expandable
                query_model = self.__join_expandable_grouped(query_model1, query_model2)#, self.join_type)
        else:  # ds1 is grouped
            if self.second_dataset.type() == "ExpandableDataset":  # ds2 is expandable while ds1 is grouped
                # move everything we joined so far to query_model2
                query_model2.prefixes = copy.copy(query_model1.prefixes)
                query_model2.from_clause = copy.copy(query_model1.from_clause)
                query_model2.select_columns = copy.copy(query_model1.select_columns)
                query_model2.variables = copy.copy(query_model1.variables)
                query_model2.offset = copy.copy(query_model1.offset)
                query_model2.limit = copy.copy(query_model1.limit)
                query_model2.order_clause = copy.copy(query_model1.order_clause)
                QueryModel.clean_inner_qm(query_model1)
                #query_model2.filter_clause = query_model1.filter_clause
                #if self.join_type == JoinType.LeftOuterJoin:
                #    query_model = self.__join_expandable_grouped(query_model2, query_model1)#, JoinType.RightOuterJoin)
                #elif self.join_type == JoinType.RightOuterJoin:
                #    query_model = self.__join_expandable_grouped(query_model2, query_model1)#, JoinType.LeftOuterJoin)
                #else:
                #    query_model = self.__join_expandable_grouped(query_model2, query_model1)#, self.join_type)
                query_model = self.__join_expandable_grouped(query_model2, query_model1)#, self.join_type)
            else:  # ds2 is grouped
                query_model = self.__join_grouped_grouped(query_model1, query_model2)
        return query_model

    def __join_expandable_expandable(self, query_model1, query_model2):
        # TODO: Union the graphs in ds2 with ds1 and assign each graph pattern to a graph in
        #  case of joining two different graphs


        if self.join_type == JoinType.InnerJoin:
            # add the basic graph patterns in dataset2 to dataset1
            for triple in query_model2.triples:
                query_model1.add_triple(*triple)
            # append the optional patterns in dataset2 to optionals in dataset1
            for op_triple in query_model2.optionals:
                query_model1.add_optional(*op_triple)
            # add the filter graph patterns of dataset2 to dataset1
            for column, conditions in query_model2.filter_clause.items():
                for condition in conditions:
                    query_model1.add_filter_condition(column, condition)

        elif self.join_type == JoinType.LeftOuterJoin:
            # add the basic and optionals graph patterns of dataset2 to dataset1 optionals
            for triple in query_model2.triples:
                query_model1.add_optional(*triple)
            # TODO: change the structure of the optional block; the optional in the original query in first block then
            #  the optional block from the second query model
            for triple in query_model2.optionals:
                query_model1.add_optional(*triple)
            # TODO: change this to add it to the optional block in querymodel1: add the filter graph patterns of dataset2 to dataset1
            for column, conditions in query_model2.filter_clause.items():
                for condition in conditions:
                    query_model1.add_filter_condition(column, condition)

        elif self.join_type == JoinType.RightOuterJoin:
            # move all triples of dataset1 to optional
            for triple in query_model1.triples:
                query_model1.add_optional(*triple)
            query_model1.rem_all_triples()
            # add the triples in dataset2 to triples in dataset1
            for triple in query_model2.triples:
                query_model1.add_triple(*triple)
            # append the optional patterns in dataset2 to optionals in dataset1
            for op_triple in query_model2.optionals:
                query_model1.add_optional(*op_triple)
            # TODO: change this to add it to the optional block in querymodel2: add the filter graph patterns of dataset2 to dataset1
            for column, conditions in query_model2.filter_clause.items():
                for condition in conditions:
                    query_model1.add_filter_condition(column, condition)

        else:  # outer join
            # The join will build three queries and two sub-queries
            # one contains the basic pattrens from the two dataset into one query
            # TODO: in case of 2 graphs
            ds1_query_model = QueryModel()
            ds2_query_model = QueryModel()
            outer_query_model = QueryModel()

            for triple in query_model1.triples:
                ds1_query_model.add_triple(*triple)
                #inner_query_model.add_triple(*triple)
            for op_triple in query_model1.optionals:
                ds1_query_model.add_optional(*op_triple)
                #inner_query_model.add_optional(*op_triple)

            for column, conditions in query_model1.filter_clause.items():
                for condition in conditions:
                    ds1_query_model.add_filter_condition(column, condition)

            for triple in query_model2.triples:
                ds2_query_model.add_triple(*triple)
                #inner_query_model.add_triple(*triple)

            for op_triple in query_model2.optionals:
                ds2_query_model.add_optional(*op_triple)
                #inner_query_model.add_optional(*op_triple)

            for column, conditions in query_model2.filter_clause.items():
                for condition in conditions:
                    ds2_query_model.add_filter_condition(column, condition)

            # add the two queries into the union of the main query
            outer_query_model.prefixes = copy.copy(query_model1.prefixes)
            outer_query_model.from_clause = copy.copy(query_model1.from_clause)
            outer_query_model.select_columns = copy.copy(query_model1.select_columns)
            outer_query_model.variables = copy.copy(query_model1.variables)
            outer_query_model.offset = copy.copy(query_model1.offset)
            outer_query_model.limit = copy.copy(query_model1.limit)
            outer_query_model.order_clause = copy.copy(query_model1.order_clause)
            query_model1.select_columns = query_model1.select_columns.union(query_model2.select_columns)

            outer_query_model.add_unions(ds1_query_model)
            outer_query_model.add_unions(ds2_query_model)
            query_model1 = outer_query_model

        return query_model1

    # first query_model1 comes always from an expandable dataset and query_model2 from a grouped dataset
    def __join_expandable_grouped(self, query_model1, query_model2):
        if self.join_type == JoinType.InnerJoin:
            # add query model 2 as a subquery
            query_model1.add_subquery(query_model2)
        elif self.join_type == JoinType.LeftOuterJoin:
            # make the subquery optional
            query_model1.add_optional_subquery(query_model2)
        elif self.join_type == JoinType.RightOuterJoin:
            # move all triples of dataset1 to optional
            #for triple in query_model1.triples:
            #    query_model1.add_optional(*triple)
            #query_model1.rem_all_triples()
            # add query model 2 as a subquery
            #query_model1.add_subquery(query_model2)
            query_model2.add_subquery(query_model1)
            return query_model2
        else:  # outer join
            # Union query model 1 with query model 2
            query_model1 = query_model1.add_unions(query_model2)
        return query_model1

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
