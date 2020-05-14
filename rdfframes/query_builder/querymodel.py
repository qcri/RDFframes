import weakref
from orderedset import OrderedSet
from collections import OrderedDict
import copy

from rdfframes.utils.helper_functions import is_uri
from rdfframes.query_builder.sparqlbuilder import SPARQLBuilder


__author__ = """
Ghadeer Abuoda <gabuoda@hbku.edu.qa>
Aisha Mohamed <ahmohamed@qf.org.qa>
Zoi Kaoudi <zkaoudi@hbku.edu.qa>
"""


class QueryModel(object):
    """
       The QueryModel class represents the intermediate object between the DAG graph and the ultimate SPARQL query.
       """
    def __init__(self):
        """
        Initializing the QueryModel
        QueryModel is a representation of a sparql query. It has a place holder for every
        possible componne of a sparql query
        """

        self.prefixes = {}          # a dictionary of prefix_name: prefix_URI

        self.variables = set()      # a set of all variables in the query.
        self.from_clause = set()       # a list of graph URIs
        self.graph_clause = {}     # a dict of graph: subquery
        self.optional_graph_clause = {}
        self.filter_clause = {}     # a dictionary of column name as key and associated conditions as a value
        self.groupBy_columns = OrderedSet() # a set of columns for the groupby modifier, it's a subset of self.variables
        self.aggregate_clause = {}  # a dictionary of new_aggregation_col_name: (aggregate function, src_column_name)
        self.having_clause = {}     # a dictionary of new_aggregation_col_name : condition
        self.order_clause = OrderedDict() # a dictionary of columns and the specifier (ASC, DSC)

        self.limit = 0              # represents the number of rows to be returned by the query.
        self.offset = 0             # represents the offset in terms of the number of rows

        self.triples = []           # list of basic graph patterns in the form (subject, predicate, object) tuples
        self.optionals = []         # list of optional query models.
        self.subqueries = []        # list of subqueries. each subquery is a query model
        self.optional_subqueries = []  # list of optional subqueries. each subquery is a query model
        self.unions = []            # list of subqueries to union with the current query model
        self.graph_triples = {}     # dict of graph: list of triples. When there is more than one triple in the graph

        self.select_columns = OrderedSet()    # list of columns to be selected ,  set()
        self.auto_generated_select_columns = OrderedSet()
        self.select_all = False

        self.querybuilder = None # a SPARQLbuilder that converts the query model to a string
        self.parent_query_model = None # a pointer to the parent query if this is a subquery
        self.is_optional = False

    def add_prefixes(self, prefixes):
        """
        Add a dictionary of prefixs to the sparql queries
        :param prefixes: a dictionary of prefixes where the key is the prefix name and the value is the prefix URI
        """
        if not self.is_subquery():
            self.prefixes.update(prefixes)

    def add_graphs(self, graphs):
        """
        Add a list of graphs to the from clause
        :param graphs: a list of graphs' URIs
        """
        if not self.is_subquery():
            self.from_clause = self.from_clause.union(graphs) #extend

    def add_graph_clause(self, query_model):
        graph = next(iter(query_model.from_clause))
        QueryModel.clean_inner_qm(query_model)
        self.graph_clause[graph] = query_model

    def add_optional_graph_clause(self, query_model):
        graph = next(iter(query_model.from_clause))
        QueryModel.clean_inner_qm(query_model)
        self.optional_graph_clause[graph] = query_model

    def add_optional_triples(self, triples, graph=None):
        """
         add a triple to the list of the optional triples in the query model.
         :param subject: subject of the triple
         :param object: object of the triple
         :param predicate: predicate of the triple
         """
        if len(triples) > 0:
            optional_query_model = OptionalQueryModel()
            for (subject, predicate, object) in triples:
                optional_query_model.add_triple(subject, predicate, object)
            self.optionals.append(optional_query_model)
            return optional_query_model

    def add_optional_block(self, optional_query_model):
        """
         add a triple to the list of the optional triples in the query model.
         :param subject: subject of the triple
         :param object: object of the triple
         :param predicate: predicate of the triple
         """
        self.optionals.append(optional_query_model)

    def add_triple(self, subject, predicate, object):
        """
         add a triple to the list of the triples in the query model.
         :param subject: subject of the triple
         :param object: object of the triple
         :param predicate: predicate of the triple
         """
        if (subject, predicate, object) not in self.triples:
            self.triples.append((subject, predicate, object))
            self.add_variable(subject)
            self.add_variable(object)
            self.add_variable(predicate)
    def add_graph_triple(self, graph, triples):
        self.graph_triples[graph] = triples

    def add_unions(self, unionquery):  # subquery type is query_builder
        """
        adds a subquery to the query model
        :param subquery:
        :return:
        """
        if len(unionquery.select_columns)<= 0 and len(unionquery.auto_generated_select_columns)<= 0:
        #if len(unionquery.select_columns) <= 0:
            unionquery.select_all = True
        self.unions.append(unionquery)
        unionquery.parent_query_model = weakref.ref(self)
        #unionquery.from_clause.clear()


    def add_subquery(self, subquery):   # subquery type is query_builder
        """
        adds a subquery to the query model
        :param subquery:
        :return:
        """
        self.subqueries.append(subquery)
        subquery.parent_query_model = weakref.ref(self)
        subquery.from_clause.clear()

    def add_optional_subquery(self, subquery):   # subquery type is query_builder
        """
        adds a subquery to the query model
        :param subquery:
        :return:
        """
        self.optional_subqueries.append(subquery)
        subquery.parent_query_model = weakref.ref(self)
        subquery.from_clause.clear()

    def add_variable(self, col_name):
        """
        add a variable (column name) to the list of the variables of a single SPARQL query (mainly to represent
         Select variables) .
         :param col_name: represents the column name after being parced from the corresponding DAG node.

        """
        if not is_uri(col_name):
            if col_name.find(":") < 0:
                self.variables.add(col_name)
            elif col_name[:col_name.find(":")] not in self.prefixes:
                self.variables.add(col_name)

    def add_group_columns(self, col_names):
        """
         add a columns  to the list of the group by columns.
        :param col_names: represents the column name that will group the records based on it.
        """
        self.groupBy_columns = self.groupBy_columns.union(col_names)

    def add_aggregate_pair(self, src_col_name, func_name, new_col_name, agg_param=None):
        """
         add a pair of column, function name to the list that forms the aggregation clause
         :param src_col_name: the source column name to be aggregated
         :param new_col_name: the new column name
         :param func_name: represents aggregation function on the corresponding column
         :param agg_param: aggregation parameter like distinct with count
         """
        if new_col_name not in self.aggregate_clause:
            self.aggregate_clause[new_col_name] = []
        self.aggregate_clause[new_col_name].append((func_name, agg_param, src_col_name))
        self.variables.add(new_col_name)

    def add_filter_condition(self, col_name, condition):
        """
        add a pair of (column, condition) to the list of conditions of the filter clause
        :param col_name: represents the column name at which the condition will be applied.
        :param condition: represents the filtering criterion ( Operator, Value)
         """
        if col_name not in self.variables:
            self.add_variable(col_name)
        if col_name in self.filter_clause:
            self.filter_clause[col_name].append(condition)
        else:
            self.filter_clause[col_name] = [condition]

    def add_having_condition(self, agg_col_name, condition):
        """
        add a pair of (column, condition) to the list of conditions of the filter clause
        :param agg_col_name: represents the column name where the filtering will occur.
        :param condition: represents the having criterion ( Operator Value)
        """
        if agg_col_name not in self.having_clause:
            self.having_clause[agg_col_name] = []
        func_name, agg_param, src_col_name = self.aggregate_clause[agg_col_name][0]
        self.having_clause[agg_col_name].append([func_name, agg_param, src_col_name, condition])

    def add_order_columns(self, sorting_cols):
        """
        add a pair of (column, specifier) to the list of sorting options.
        :param sorting_cols: list of pairs of (column name, sort order) that will be used for sorting
        """
        for col, order in sorting_cols.items():
            self.order_clause[col] = order

    def set_limit(self, limit):
        """
        :param limit: the value that represents the number of results to be returned
        :return: none
        """
        self.limit = limit

    def set_offset(self, offset):
        """

        :param offset: the value that represents the number of NEXT results to be returned
        :return: none
        """
        self.offset = offset

    def add_select_column(self, col_name):
        """
        :param col_name:
        :return:
        """
        self.select_columns.add(col_name)

    def auto_add_select_column(self, col_name):
        #print("Auto adding {}".format(col_name))
        self.auto_generated_select_columns.add(col_name)

    def rem_select_column(self, col_name):
        self.select_columns.remove(col_name)

    def rem_all_triples(self):
        self.triples = []

    def rem_graph_triples(self):
        self.graph_triples = {}

    def rem_from_clause(self):
        self.from_clause = set()

    def rem_prefixes(self):
        self.prefixes = {}

    def rem_optional_triples(self):
        self.optionals = []

    def rem_filters(self):
        self.filter_clause = {}

    def rem_subqueries(self):
        self.subqueries = []

    def rem_optional_subqueries(self):
        self.optional_subqueries  = []

    def rem_unions(self):
        self.unions = []

    def transfer_grouping_to_subquery(self, subquery):
        grouping_cols = self.groupBy_columns

        for g_col in grouping_cols:
            involved_triples = [triple for triple in self.triples
                                if g_col == triple[0] or g_col == triple[2]]
            for t in involved_triples:
                subquery.add_triple(*t)

        subquery.groupBy_columns = OrderedSet(grouping_cols)
        subquery.select_columns = set(grouping_cols)
        subquery.having_clause = dict(self.having_clause)
        subquery.aggregate_clause = dict(self.aggregate_clause)

        self.groupBy_columns.clear()
        self.having_clause.clear()
        self.aggregate_clause.clear()
        self.add_subquery(subquery)

    @staticmethod
    def clean_inner_qm(qm):
        # clean the inner query (self)
        qm.rem_prefixes()
        qm.rem_from_clause()
        qm.limit = 0
        qm.offset = 0
        qm.order_clause = OrderedDict()

    def wrap_in_a_parent_query(self):
        """
        wraps the current query in a subquery and returns a new query model that contains one graph pattern which is
        the current query as a subquery
        :return: a new QueryModel that wraps the current query model
        """
        # initialize the parent query with the graph uri, the prefixes and the variables in the inner query
        parent_query = QueryModel()
        parent_query.add_prefixes(self.prefixes)
        parent_query.add_graphs(self.from_clause)
        to_add_to_select = []
        to_rem_from_select = []
        for var in self.select_columns:
            # if select column in groupby or aggregation: add it to selected columns by the user in inner query
            #  and the outer query.
            #  else: find the relevant graph patterns and move them to the outer query and
            #  remove the select column from select clause in inner query
            if (var in self.groupBy_columns) or (var in self.aggregate_clause):
                to_add_to_select.append(var)
            else:
                # add basic graph patterns
                involved_triples = [triple for triple in self.triples
                                        if var == triple[0] or var == triple[2]]
                for t in involved_triples:
                    parent_query.add_triple(*t)
                # add filter patterns
                if var in self.filter_clause:
                    for condition in self.filter_clause[var]:
                        parent_query.add_filter_condition(var, condition)
                # add subqueries
                # Is it query.select or query.variables
                for subquery in self.subqueries:
                    if var in subquery.select_columns:
                        parent_query.add_subquery(subquery)
                to_rem_from_select.append(var)
            parent_query.auto_add_select_column(var)
            parent_query.add_variable(var)
        for var in to_add_to_select:
            self.auto_add_select_column(var)
        for var in to_rem_from_select:
            self.rem_select_column(var)

        # set the limit and offset of the outer query. don't allow limit and offset in the inner query
        parent_query.set_limit(self.limit)
        parent_query.set_offset(self.offset)
        parent_query.add_order_columns(self.order_clause)
        # add self to the subqueries in the parent subquery
        parent_query.add_subquery(self)
        self.parent_query_model = parent_query

        # clean the inner query (self)
        QueryModel.clean_inner_qm(self)
        #self.prefixes = {}
        #self.rem_from_clause()
        #self.limit = 0
        #self.offset = 0
        #self.order_clause = OrderedDict()
        #self.rem_from_clause()

        return parent_query

    def transfer_select_triples_to_parent_query(self, parent_ds_cols):
        # transfer the order by, the filter clause,
        for col in parent_ds_cols:
            triples_list = [subquery.triples for subquery in self.subqueries]
            involved_triples = [triple for triples in triples_list for triple in triples  if col == triple[0] or col == triple[2]]
            for t in involved_triples:
                if t not in self.triples:
                    self.add_triple(*t)


    def is_defined_variable(self, var):
        return var in self.variables or any([subquery.is_defined_variable(var) for subquery in self.subqueries])

    def is_grouped(self):
        return len(self.groupBy_columns) > 0

    def is_sorted(self):
        return len(self.order_clause) > 0

    def to_sparql(self):
        #self.validate()
        self.querybuilder = SPARQLBuilder()
        return self.querybuilder.to_sparql(self)

    def is_aggregate_col(self, src_col_name):
        if src_col_name in self.aggregate_clause:
            return True
        return False

    def is_subquery(self):
        return self.parent_query_model is not None

    def all_variables(self):
        if len(self.subqueries) == 0:
            return self.variables

        all_vars = set().union(self.variables)

        for subq in self.subqueries:
            all_vars = all_vars.union(subq.all_variables())
        return all_vars

    def rename_variable(self, old_name, new_name):
        self.triples = [[new_name if element == old_name else element for element in triple] for triple in self.triples]
        for query in self.optionals:
            query.rename_variable(old_name, new_name)
        self.select_columns = OrderedSet([new_name if var == old_name else var for var in self.select_columns])
        self.auto_generated_select_columns = OrderedSet([new_name if var == old_name else var for var in self.auto_generated_select_columns])
        self.groupBy_columns = OrderedSet([new_name if var == old_name else var for var in self.groupBy_columns])
        self.variables = {new_name if var == old_name else var for var in self.variables}
        if old_name in self.order_clause:
            self.order_clause[new_name] = self.order_clause[old_name]
            del self.order_clause[old_name]
        if old_name in self.filter_clause:
            self.filter_clause[new_name] = self.filter_clause[old_name]
            del self.filter_clause[old_name]
        if old_name in self.having_clause:
            self.having_clause[new_name] = self.having_clause[old_name]
            del self.having_clause[old_name]
        for var in self.aggregate_clause:
            self.aggregate_clause[var] = [[new_name if element == old_name else element
                                           for element in triple] for triple in self.aggregate_clause[var]]
            if var == old_name:
                self.aggregate_clause[new_name] = self.aggregate_clause[old_name]
                del self.aggregate_clause[old_name]
        for query in self.subqueries:
            query.rename_variable(old_name, new_name)
        for query in self.unions:
            query.rename_variable(old_name, new_name)
        for query in self.optional_subqueries:
            query.rename_variable(old_name, new_name)

    def is_valid_prefix(self, prefix):
        if prefix in self.prefixes.keys():
            return True
        else:
            return False
    """
    def get_triples(self):
        triple_string = ""
        for triple in self.triples:
            triple1 = triple[1]
            triple2 = triple[2]
            if not is_uri(triple[1]) and triple[1].find(":") < 0:
                triple1 = "?" + triple[1]
            if not is_uri(triple[2]) and triple[2].find(":") < 0:
                triple2 = "?" + triple[2]
            triple = (triple[0], triple1, triple2)
            triple_string += '\t?%s %s %s' % (triple[0], triple[1], triple[2]) + " .\n"
        optional_string = self.get_optional_triples()
        triple_string += '\t'.join(('\n' + optional_string.lstrip()).splitlines(True))
        return triple_string

    def get_optional_triples(self):
        optional_string = ""
        if len(self.optionals) > 0:
            optional_string = "OPTIONAL {  \n"
            for triple in self.optionals:
                triple1 = triple[1]
                triple2 = triple[2]
                if not is_uri(triple[1]) and triple[1].find(":") < 0:
                    triple1 = "?" + triple[1]
                if not is_uri(triple[2]) and triple[2].find(":") < 0:
                    triple2 = "?" + triple[2]
                triple = (triple[0], triple1, triple2)
                optional_string += '\t?%s %s %s' % (triple[0], triple[1], triple[2]) + " .\n"
            optional_string += "}"

        return optional_string
        """

    def union(self, qm2):
        """
        union this query model with query model (qm2)
        :param qm2:
        :return: a query model that unions the current query model and qm2
        """
        final_qm = QueryModel()

        if self.from_clause == qm2.from_clause:  # same graph
            # add the graphs to the outer qm and remove them  from the inner qms
            final_qm.add_graphs(self.from_clause)
            final_qm.add_graphs(qm2.from_clause)

            # union the prefixes and remove them  from the inner qms
            # TODO: check that all namespaces that have the same prefix have the same uri
            final_qm.add_prefixes(self.prefixes)
            final_qm.add_prefixes(qm2.prefixes)

            final_qm.variables = final_qm.variables.union(self.variables)
            final_qm.variables = final_qm.variables.union(qm2.variables)

            final_qm.set_offset(min(self.offset, qm2.offset))
            final_qm.set_limit(max(self.limit, qm2.limit))
            final_qm.add_order_columns(self.order_clause)
            final_qm.add_order_columns(qm2.order_clause)

            QueryModel.clean_inner_qm(self)
            QueryModel.clean_inner_qm(qm2)

            final_qm.add_unions(self)
            final_qm.add_unions(qm2)

            return final_qm

    def validate(self):
        """
        validate the columns and parameters data in the query model and reports and inconsistencies.
        1) validate the namespance in the preidcate to match the given in the graphs' prefixes
        2)
        :return: True if valid and False if not
        """
        ## add the aggregation to expandable group

        ## group by in expandable dataset, raise exception if the select cols not in the group by
        ### validate the prefix in the triple
        if self.parent_query_model is None:
            for triple in self.triples:
                if not is_uri(triple[1]):
                    if triple[1].find(":") >= 0:
                        prefix= triple[1].split(":")
                        if(len(prefix)>=1):
                            if not self.is_valid_prefix(prefix[0]):
                                raise Exception("Not a valid Prefix in triple {}".format(triple))
                    else:
                        # predicate is a variable
                        pass

        if self.parent_query_model is None:
            for col_name in  self.filter_clause:
                if col_name.find(':') != -1:
                    prefix = col_name.split(":")
                    if (len(prefix) >= 1):
                        if not self.is_valid_prefix(prefix[0]):
                            raise Exception("Not a valid Prefix in filter {}".format(col_name))


        for subquery in self.subqueries:
            subquery_variables_set = set(subquery.variables)
            my_variables_set = set(self.variables)
            intersection_variables = my_variables_set.intersection(subquery_variables_set)
            if len(intersection_variables) < 0:
                raise Exception("No common variables between the main query and the subquery")

        all_vars = self.all_variables()
        missing_vars = set()

        for sel_col in self.select_columns:
            if sel_col not in all_vars:
                missing_vars.add(sel_col)

        if len(missing_vars) > 0:
            raise Exception('Variables {} are not defined in the query\'s body'.format(', '.join(missing_vars)))

        # filter_clause validation
        for col_name in  self.filter_clause:
            if col_name not in all_vars:
                raise Warning('Cannot add filter on {}, is not part of the query variables'.format(col_name))

        for col in self.order_clause:
            if col not in self.variables:
                raise Warning('{} cannot be a sorting column, it should be part of variables'.format(col))

        for col_name in self.having_clause:
            if not self.is_aggregate_col(col_name):
                raise Warning('{} is not an aggregate column, cannot be added to having clause'.format(col_name))

    def copy(self):
        return copy.deepcopy(self)


    def __repr__(self):
        return self.to_sparql()

    def __str__(self):
        return self.to_sparql()


class OptionalQueryModel(QueryModel):
    """
    The optional block class. It contains mutliple SPARQL patterns that are all included in one optional block
    Allows for multiple patterns inside one optional block and for nested optionals.
    """
    def __init__(self):
        super(OptionalQueryModel, self).__init__()
        self.is_optional = True