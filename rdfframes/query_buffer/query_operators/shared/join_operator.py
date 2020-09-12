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
        # rename all variables of the first and second query models if necessary
        if self.src_col_name != self.new_col_name:
            query_model1.rename_variable(self.src_col_name, self.new_col_name)
        if self.second_col_name != self.new_col_name:
            query_model2.rename_variable(self.second_col_name, self.new_col_name)

        # if join between two different graphs
        if query_model1.from_clause != query_model2.from_clause:
            if len(query_model2.from_clause) > 1 or len(query_model1.from_clause) > 1:
                raise Exception("Can't join two datasets that were created from multiple graphs. GRAGH keyword "
                                "takes only with more than one graph")
            # create an outer query model
            query_model = JoinOperator.__create_outer_quer_model(query_model1, query_model2)
            if not self.dataset.is_grouped: #if self.dataset.type() == "ExpandableDataset":
                if not self.second_dataset.is_grouped: # two expandable datasets
                    query_model = self.__join_expandable_expandable_2_graphs(query_model, query_model1, query_model2)
                else:  # ds2 is grouped while ds1 is expandable
                    query_model = self.__join_expandable_grouped_2_graphs(query_model, query_model1, query_model2, 1)
                    #self.dataset.is_grouped = True
            else:  # ds1 is grouped while ds2 is expandable
                if not self.second_dataset.is_grouped:
                    query_model = self.__join_expandable_grouped_2_graphs(query_model, query_model1, query_model2, 2)
                else:  # ds2 is grouped
                    query_model = self.__join_grouped_grouped_2_graphs(query_model, query_model1, query_model2)
            return query_model
        else: # join two datasets extracted from the same graph
            # make query model 1 the outer query model the prefixes
            if not self.dataset.is_grouped:
                if not self.second_dataset.is_grouped: # two expandable datasets
                    query_model = self.__join_expandable_expandable(query_model1, query_model2)
                else:  # ds2 is grouped while ds1 is expandables
                    query_model = self.__join_expandable_grouped(query_model1, query_model2, expandable_order=1)
                    #self.dataset.is_grouped = True
            else:  # ds1 is grouped
                if not self.second_dataset.is_grouped:  # ds2 is expandable while ds1 is grouped
                    query_model = self.__join_expandable_grouped(query_model1, query_model2, expandable_order=2)
                else:  # ds2 is grouped
                    query_model = self.__join_grouped_grouped(query_model1, query_model2)
            return query_model

    def __join_expandable_expandable_2_graphs(self, query_model, query_model1, query_model2):
        #if len(query_model1.aggregate_clause) > 0 or len(query_model2.aggregate_clause) > 0:
        #    raise Exception("Can't join flat aggregated datasets")
        if self.join_type == JoinType.InnerJoin:
            query_model.add_graph_clause(query_model1)
            query_model.add_graph_clause(query_model2)
            return query_model
        elif self.join_type == JoinType.LeftOuterJoin:
            # add the basic and optionals graph patterns of dataset2 to dataset1 in one optional block
            query_model.add_graph_clause(query_model1)
            query_model.add_optional_graph_clause(query_model2)
            return query_model
        elif self.join_type == JoinType.RightOuterJoin:
            query_model.add_graph_clause(query_model2)
            query_model.add_optional_graph_clause(query_model1)
            return query_model
        else:  # outer join
            return JoinOperator._outer_join_two_graphs(query_model, query_model1, query_model2)

    def __join_expandable_grouped_2_graphs(self, query_model, query_model1, query_model2, expandable_order=1):
        if self.join_type == JoinType.OuterJoin:  # outer join
            return JoinOperator._outer_join(query_model, query_model1, query_model2)
        elif self.join_type == JoinType.InnerJoin:
            # add query model 2 as a subquery
            if expandable_order == 1:
                old_query_model = query_model2
                first = query_model1
            else:
                assert (expandable_order == 2)
                old_query_model = query_model1
                first = query_model2
            new_query_model = JoinOperator.__wrap_in_subquery(old_query_model)
            new_query_model.add_subquery(old_query_model)
            return self.__join_expandable_expandable_2_graphs(query_model, first, new_query_model)

        elif ((expandable_order == 1 and self.join_type == JoinType.LeftOuterJoin) or \
              (expandable_order == 2 and self.join_type == JoinType.RightOuterJoin)):
            if expandable_order == 1:
                old_query_model = query_model2
                first = query_model1
            else:
                assert(expandable_order ==2)
                old_query_model = query_model1
                first = query_model2
            new_query_model = JoinOperator.__wrap_in_subquery(old_query_model)
            new_query_model.add_subquery(old_query_model)
            return self.__join_expandable_expandable_2_graphs(query_model, first, new_query_model)

        elif ((expandable_order == 2 and self.join_type == JoinType.LeftOuterJoin) or \
              (expandable_order == 1 and self.join_type == JoinType.RightOuterJoin)):
            new_query_model1 = JoinOperator.__wrap_in_subquery(query_model1)
            new_query_model1.add_subquery(query_model1)

            new_query_model2 = JoinOperator.__wrap_in_subquery(query_model2)
            new_query_model2.add_subquery(query_model2)

            query_model.add_optional_graph_clause(new_query_model1)
            query_model.add_graph_clause(new_query_model2)
            return query_model
        else:
            raise Exception("Undefined case of grouped and expandable datasets joins")

    def __join_grouped_grouped_2_graphs(self, query_model, query_model1, query_model2):
        query_model1_copy = copy.deepcopy(query_model1)
        new_query_model1 = JoinOperator.__wrap(query_model1)
        new_query_model1.add_graph_clause(query_model1)

        query_model2_copy = copy.deepcopy(query_model2)
        new_query_model2 = JoinOperator.__wrap(query_model2)
        new_query_model2.add_graph_clause(query_model2)
        # add subqueries
        if self.join_type == JoinType.InnerJoin:
            query_model.add_graph_clause(query_model1)
            query_model.add_graph_clause(query_model2)
        elif self.join_type == JoinType.LeftOuterJoin:
            query_model.add_graph_clause(query_model1)
            query_model.add_optional_graph_clause(query_model2)
        elif self.join_type == JoinType.RightOuterJoin:
            query_model.add_optional_graph_clause(query_model1)
            query_model.add_graph_clause(query_model2)
        else:  # outer join
            new_query_model1.add_optional_graph_clause(query_model2_copy)
            new_query_model2.add_optional_graph_clause(query_model1_copy)
            query_model.add_unions(new_query_model1)
            query_model.add_unions(new_query_model2)
        return query_model

    def __join_expandable_expandable(self, query_model1, query_model2):
        # if in this function then 2 datasets are expandable and from the same graph
        if len(query_model1.aggregate_clause) > 0 or len(query_model2.aggregate_clause) > 0:
            raise Exception("Can't join flat aggregated datasets")
        if self.join_type == JoinType.InnerJoin:
            # add the basic graph patterns in dataset2 to dataset1
            for triple in query_model2.triples:
                query_model1.add_triple(*triple)
            for graph, triples in query_model2.graph_triples.items():
                query_model1.add_graph_triple(graph, triples)
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
            # TODO: if there are graphs common between the two query models, join the inner graphs
            query_model1.graph_clause.update(query_model2.graph_clause)
            query_model1.optional_graph_clause.update(query_model2.optional_graph_clause)
            return query_model1

        elif self.join_type == JoinType.LeftOuterJoin:
            # add the basic and optional graph patterns of dataset2 to dataset1 in one optional block
            query_model1 = JoinOperator.__make_parent(query_model1, query_model2)
            optional_block = query_model1.add_optional_triples(query_model2.triples)
            for graph, triples in query_model2.graph_triples.items():
                optional_block.add_graph_triple(graph, triples)
            # append the optional patterns in dataset2 to optionals in dataset1
            for optional_block in query_model2.optionals:
                optional_block.add_optional_block(optional_block)
            # add the filter graph patterns of dataset2 to dataset1
            for column, conditions in query_model2.filter_clause.items():
                for condition in conditions:
                    optional_block.add_filter_condition(column, condition)
            for query in query_model2.subqueries:
                optional_block.add_subquery(query)
            for query in query_model2.optional_subqueries:
                optional_block.add_optional_subquery(query)
            for query in query_model2.unions:
                optional_block.add_unions(query)
            # TODO: if there are graphs common between the two query models, join the inner graphs
            optional_block.graph_clause.update(query_model2.graph_clause)
            optional_block.optional_graph_clause.update(query_model2.optional_graph_clause)
            # TODO: if there is a bug, uncomment these 2 lines and remove the optional_block;
            #QueryModel.clean_inner_qm(query_model2)
            #query_model1.add_optional_subquery(query_model2)
            return query_model1
        elif self.join_type == JoinType.RightOuterJoin:
            query_model2 = JoinOperator.__make_parent(query_model2, query_model1)
            optional_block = query_model2.add_optional_triples(query_model1.triples)
            for graph, triples in query_model1.graph_triples.items():
                optional_block.add_graph_triple(graph, triples)
            # append the optional patterns in dataset2 to optionals in dataset1
            for optional_block in query_model1.optionals:
                optional_block.add_optional_block(optional_block)
            # add the filter graph patterns of dataset2 to dataset1
            for column, conditions in query_model1.filter_clause.items():
                for condition in conditions:
                    optional_block.add_filter_condition(column, condition)
            for query in query_model1.subqueries:
                optional_block.add_subquery(query)
            for query in query_model1.optional_subqueries:
                optional_block.add_optional_subquery(query)
            for query in query_model1.unions:
                optional_block.add_unions(query)
            # TODO: if there are graphs common between the two query models, join the inner graphs
            optional_block.graph_clause.update(query_model1.graph_clause)
            optional_block.optional_graph_clause.update(query_model1.optional_graph_clause)
            # TODO: if there is a bug, uncomment these 2 lines and remove the optional_block;
            #QueryModel.clean_inner_qm(query_model1)
            #query_model2.add_optional_subquery(query_model1)
            return query_model2
        else:  # outer join
            # TODO: could be made simpler
            query_model = JoinOperator.__create_outer_quer_model(query_model1, query_model2)
            return JoinOperator._outer_join(query_model, query_model1, query_model2)

    # first query_model1 comes always from an expandable dataset and query_model2 from a grouped dataset
    def __join_expandable_grouped(self, query_model1, query_model2, expandable_order=1):
        if self.join_type == JoinType.OuterJoin:  # outer join
            joined_query_model = JoinOperator.__create_outer_quer_model(query_model1, query_model2)
            return JoinOperator._outer_join(joined_query_model, query_model1, query_model2)
        elif self.join_type == JoinType.InnerJoin :
                # add query model 2 as a subquery
                if expandable_order == 1:
                    query_model1 = JoinOperator.__make_parent(query_model1, query_model2)
                    QueryModel.clean_inner_qm(query_model2)
                    query_model1.add_subquery(query_model2)
                    return query_model1
                else:
                    query_model2 = JoinOperator.__make_parent(query_model2, query_model1)
                    QueryModel.clean_inner_qm(query_model1)
                    query_model2.add_subquery(query_model1)
                    return query_model2
        elif ((expandable_order == 1 and self.join_type == JoinType.LeftOuterJoin) or\
            (expandable_order == 2 and self.join_type == JoinType.RightOuterJoin)):
            # make query model 2 an optional subquery
            if expandable_order == 1:
                query_model1 = JoinOperator.__make_parent(query_model1, query_model2)
                QueryModel.clean_inner_qm(query_model2)
                query_model1.add_optional_subquery(query_model2)
                return query_model1
            else:
                query_model2 = JoinOperator.__make_parent(query_model2, query_model1)
                QueryModel.clean_inner_qm(query_model1)
                query_model2.add_optional_subquery(query_model1)
                return query_model2
        elif ((expandable_order == 2 and self.join_type == JoinType.LeftOuterJoin) or\
            (expandable_order == 1 and self.join_type == JoinType.RightOuterJoin)):
            # create an outer query and add the main dataset as a subquery and the optional dataset as optional subquery
            if expandable_order == 1:
                expandable_qm, grouped_qm = query_model1, query_model2
            else:
                expandable_qm, grouped_qm = query_model2, query_model1
            joined_query_model = JoinOperator.__wrap_in_subquery(expandable_qm)
            joined_query_model = JoinOperator.__make_parent(joined_query_model, grouped_qm)
            QueryModel.clean_inner_qm(expandable_qm)
            QueryModel.clean_inner_qm(grouped_qm)
            joined_query_model.add_optional_subquery(expandable_qm)
            joined_query_model.add_subquery(grouped_qm)
            return joined_query_model
        else:
            raise Exception("Undefined case of expandable grouped join")

    def __join_grouped_grouped(self, query_model1, query_model2):
        joined_query_model = JoinOperator.__create_outer_quer_model(query_model1, query_model2)
        joined_query_model.from_clause = copy.copy(query_model1.from_clause)
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
            query_model1.from_clause = copy.copy(joined_query_model.from_clause)
            return JoinOperator._outer_join(joined_query_model, query_model1, query_model2)
        return joined_query_model

    def __repr__(self):
        """
        return: node to string representation
        """
        return "Join Operator dataset1: {}  dataset2: {} col1: {} col2: {} join type: {}".format(
            self.src_datset_name, self.second_dataset.name, self.src_col_name, self.second_col_name, self.join_type)

    @staticmethod
    def _outer_join(joined_query_model, query_model1, query_model2):
        joined_query_model.from_clause = copy.copy(query_model1.from_clause)
        query_model1_copy = copy.deepcopy(query_model1)
        query_model2_copy = copy.deepcopy(query_model2)
        #if len(query_model1.groupBy_columns) > 0:
        if True:
            new_query_model1 = JoinOperator.__wrap_in_subquery(query_model1)
            QueryModel.clean_inner_qm(query_model2_copy)
            QueryModel.clean_inner_qm(query_model1_copy)
            new_query_model1.add_subquery(query_model1_copy)
            new_query_model1.add_optional_subquery(query_model2_copy)
        else:
            query_model1.add_optional_subquery(query_model2_copy)
        #if len(query_model2.groupBy_columns) > 0:
        if True:
            new_query_model2 = JoinOperator.__wrap_in_subquery(query_model2)
            new_query_model2.add_subquery(query_model2_copy)
            new_query_model2.add_optional_subquery(query_model1_copy)
        else:
            query_model2.add_optional_subquery(query_model1_copy)
        joined_query_model.add_unions(new_query_model1)
        joined_query_model.add_unions(new_query_model2)
        return joined_query_model


    @staticmethod
    def __create_outer_quer_model(query_model1, query_model2):
        """
        Joins the prefixes, offset, limit, order and select clauses and variables of the two query models.
        Doesn't add the from clause from both variables
        :param query_model1:
        :param query_model2:
        :return: an outer query model
        """
        query_model = QueryModel()
        #query_model.from_clause.clear()
        query_model.add_prefixes(query_model1.prefixes)
        query_model.add_prefixes(query_model2.prefixes)

        query_model.set_offset(min(query_model1.offset, query_model2.offset))
        query_model.set_limit(max(query_model1.limit, query_model2.limit))
        query_model.add_order_columns(query_model1.order_clause)
        query_model.add_order_columns(query_model2.order_clause)

        # TODO: WHY do this here?
        # union the select columns
        if len(query_model1.select_columns) > 0 and len(query_model1.select_columns) > 0:
            query_model.select_columns = query_model1.select_columns.union(query_model2.select_columns)
        elif len(query_model1.select_columns) > 0:
            query_model.select_columns = query_model1.select_columns.union(query_model2.variables)
        elif len(query_model2.select_columns) > 0:
            query_model.select_columns = query_model1.variables.union(query_model2.select_columns)
        # union the variables
        query_model.variables = query_model1.variables.union(query_model2.variables)

        return query_model

    @staticmethod
    def __wrap(query_model):
        """
        create an outer query containing the variables, select columns, offset, limit, order,
        :param query_model:
        :return:
        """
        new_query_model = QueryModel()
        new_query_model.variables = copy.copy(query_model.variables)  # all prefixes are already in query_model1
        new_query_model.from_clause = copy.copy(query_model.from_clause)
        new_query_model.select_columns = copy.copy(query_model.select_columns)
        new_query_model.offset = query_model.offset
        new_query_model.limit = query_model.limit
        new_query_model.order_clause = copy.copy(query_model.order_clause)
        new_query_model.add_graph_clause(query_model)
        QueryModel.clean_inner_qm(query_model)
        return new_query_model

    @staticmethod
    def __wrap_in_subquery(query_model):
        """
        create an outer query containing the variables, select columns, offset, limit, order,
        :param query_model:
        :return:
        """
        new_query_model = QueryModel()
        new_query_model.variables = copy.copy(query_model.variables)  # all prefixes are already in query_model1
        new_query_model.from_clause = copy.copy(query_model.from_clause)
        new_query_model.select_columns = copy.copy(query_model.select_columns)
        new_query_model.offset = query_model.offset
        new_query_model.limit = query_model.limit
        new_query_model.order_clause = copy.copy(query_model.order_clause)
        return new_query_model



    @staticmethod
    def _outer_join_two_graphs(query_model, query_model1, query_model2):
        query_model1_copy = copy.deepcopy(query_model1)
        query_model2_copy = copy.deepcopy(query_model2)
        #if len(query_model1.groupBy_columns) > 0:
        if True:
            new_query_model1 = JoinOperator.__wrap(query_model1)
        else:
            query_model1.add_optional_graph_cluase(query_model2_copy)
        #if len(query_model2.groupBy_columns) > 0:
        if True:
            new_query_model2 = JoinOperator.__wrap(query_model2)
        else:
            query_model2.add_optional_graph_cluase(query_model1_copy)
        new_query_model1.add_optional_graph_clause(query_model2_copy)
        new_query_model2.add_optional_graph_clause(query_model1_copy)
        query_model.add_unions(new_query_model1)
        query_model.add_unions(new_query_model2)
        return query_model

    @staticmethod
    def __make_parent(parent, child):
        # make query model 1 the outer query model the prefixes
        parent.add_prefixes(child.prefixes)
        parent.add_graphs(child.from_clause)
        parent.set_offset(min(parent.offset, child.offset))
        parent.set_limit(max(parent.limit, child.limit))
        parent.add_order_columns(child.order_clause)
        # union the select columns
        if len(parent.select_columns) > 0 and len(parent.select_columns) > 0:
            parent.select_columns = parent.select_columns.union(child.select_columns)
        elif len(parent.select_columns) > 0:
            parent.select_columns = parent.select_columns.union(child.variables)
        elif len(child.select_columns) > 0:
            parent.select_columns = parent.variables.union(child.select_columns)
        parent.variables = parent.variables.union(child.variables)
        return parent

