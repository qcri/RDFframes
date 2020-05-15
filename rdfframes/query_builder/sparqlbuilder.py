from rdfframes.utils.helper_functions import is_uri

__author__ = """
Ghadeer Abuoda <gabuoda@hbku.edu.qa>
Aisha Mohamed <ahmohamed@qf.org.qa>
"""


class SPARQLBuilder(object):
        """
        This class will parse the self.self.query_modelto generate the SPARQL query.
        """
        def __init__(self):
            """
             Initializing the query_builder object
             it has a sparql_query that contains the SPARQL object
            """
            self.query_string = ""          # represents the query string of base query
            self.query_model = None

        def to_sparql(self, query_model):
            """
            :param query_model: the query_model that contains the information required to generate the query
            :return: sub_query_string or query_string depending on the type of the query model
            """
            self.query_model = query_model
            if self.query_model.is_optional:
                self.query_string = self.__add_patterns()
                return self.query_string

            if query_model.parent_query_model is None:  # if not a subquery
                self.add_prefixes()
            self.add_select()
            if query_model.parent_query_model is None:
                self.add_from()
            self.add_where_clause()
            self.add_groupby()
            self.add_having_clause()
            self.add_order_clause()
            self.add_limit()
            self.add_offset()

            self.query_string = self.query_string.rstrip("\n")
            self.query_string += "\n"

            return self.query_string

        def add_prefixes(self):
            """
             add namespaces required the SPARQL query
            """
            prefixes_string = ""
            if len(self.query_model.prefixes) > 0:
                for prefix, prefix_uri in self.query_model.prefixes.items():
                    prefixes_string += "PREFIX {}: <{}>\n".format(prefix, prefix_uri)
                self.query_string = prefixes_string

        def add_select(self):
            """
            add select columns from querymodel into the SPARQL query
            """
            if self.query_model.select_all == True:
                select_string = "SELECT * \n"
                self.query_string += select_string
                return
            elif len(self.query_model.select_columns) > 0 or len(self.query_model.auto_generated_select_columns) > 0:
            #elif len(self.query_model.select_columns) > 0:
                select_string = "SELECT DISTINCT "
                for col in self.query_model.select_columns.union(self.query_model.auto_generated_select_columns):
                #for col in self.query_model.select_columns:
                    if col in self.query_model.aggregate_clause:
                        agg_part = self.query_model.aggregate_clause[col]
                        agg_func = agg_part[0][0]
                        src_col_name = "?"+agg_part[0][2] if agg_part[0][2]!="*" else "*"
                        agg_col_name = "AS ?{}".format(col) if col is not None else ''
                        agg_parameter = agg_part[0][1] if agg_part[0][1] is not None else ''
                        select_string += " (%s(%s %s) %s) " % (agg_func, agg_parameter, src_col_name, agg_col_name)
                    else:
                        select_string += "?%s " % col
                select_string += "\n"
            else:
                select_string = "SELECT * \n"
            self.query_string += select_string

        def return_subquery(self, subquery):
            """

            :param subquery: build the query model for the subquery
            :return: the string representation of the SPARQL query for the the subquery
            """
            return self.to_sparql(subquery)

        def add_subqueries(self):
            """
            attach the subquery to its outer query
            :return: The SPARQL representation of the query
            """
            subquery_string = ""
            for query in self.query_model.subqueries:
                query_str = query.to_sparql()
                subquery_string += "\n{\n" + query_str + "}"
            return subquery_string

        def add_optional_subqueries(self):
            """
            attach the optional subquery to its outer query
            :return: The SPARQL representation of the query
            """
            subquery_string = ""
            for query in self.query_model.optional_subqueries:
                subquery_string += "\n OPTIONAL"
                query_str = query.to_sparql()
                subquery_string += "\t"+"{" + '\t'.join(('\n'+query_str.lstrip()).splitlines(True)) + "\n\t" + "\t}"
            return subquery_string

        def add_optional_clause(self):
            if len(self.query_model.optionals) > 0:
                optional_string = ""
                for optional_block in self.query_model.optionals:
                    query_string = optional_block.to_sparql()
                    optional_string += "\tOPTIONAL {" + '\t'.join(('\n'+query_string.lstrip()).splitlines(True)) + "\n\t" + "\t}"
                    #optional_string += "\tOPTIONAL {" + "{}".format(query_string) + "}\n"
                return optional_string
            else:
                return ""

        def add_graph_clause(self):
            if len(self.query_model.graph_clause) > 0:
                graph_string = ""
                query_builder = SPARQLBuilder()
                for graph_uri, graph_query in self.query_model.graph_clause.items():
                    query_builder.query_model = graph_query
                    query_string = query_builder.__add_patterns()
                    graph_string +=  "\tGRAPH <{}>".format(graph_uri) + ' { '+ '\t'.join(('\n'+query_string.lstrip()).splitlines(True)) + "\n\t" + "\t}"
                return graph_string
            else:
                return ""

        def add_optional_graph_clause(self):
            if len(self.query_model.optional_graph_clause) > 0:
                graph_string = ""
                query_builder = SPARQLBuilder()
                for graph_uri, graph_query in self.query_model.optional_graph_clause.items():
                    query_builder.query_model = graph_query
                    query_string = query_builder.__add_patterns()
                    graph_string +=  "\tOPTIONAL { " + "GRAPH <{}>".format(graph_uri) + ' { '+ '\t'.join(('\n'+query_string.lstrip()).splitlines(True)) + "\n\t" + "\t}}"
                return graph_string
            else:
                return ""


        def __add_patterns(self):
            where_string = ""
            for triple in self.query_model.triples:
                triple0 = triple[0]
                triple1 = triple[1]
                triple2 = triple[2]
                if not is_uri(triple[0]) and triple[0].find(":") < 0:
                    triple0 = "?" + triple[0]
                if not is_uri(triple[1]) and triple[1].find(":") < 0:
                    triple1 = "?" + triple[1]
                if not is_uri(triple[2]) and triple[2].find(":") < 0:
                    triple2 = "?" + triple[2]
                where_string += '\n\t{} {} {}'.format(triple0, triple1, triple2) + " ."
            if len(self.query_model.graph_triples) > 0:
                for graph, triples in self.query_model.graph_triples.items():
                    if len(triples) > 0:
                        graph_triples_string = "GRAPH <{}> ".format(graph) + "{\n"
                        for triple in triples:
                            triple0 = triple[0]
                            triple1 = triple[1]
                            triple2 = triple[2]
                            if not is_uri(triple[0]) and triple[0].find(":") < 0:
                                triple0 = "?" + triple[0]
                            if not is_uri(triple[1]) and triple[1].find(":") < 0:
                                triple1 = "?" + triple[1]
                            if not is_uri(triple[2]) and triple[2].find(":") < 0:
                                triple2 = "?" + triple[2]
                            graph_triples_string += '\t{} {} {}'.format(triple0, triple1, triple2) + " .\n"
                        graph_triples_string += " }\n"
                        where_string += "\n" + '\t'.join(('\n' + graph_triples_string.lstrip()).splitlines(True))
            if len(self.query_model.graph_clause) > 0:
                graph_string = self.add_graph_clause()
                where_string += '\t'.join(('\n' + graph_string.lstrip()).splitlines(True))
            if len(self.query_model.optional_graph_clause) > 0:
                graph_string = self.add_optional_graph_clause()
                where_string += '\t'.join(('\n' + graph_string.lstrip()).splitlines(True))
            if len(self.query_model.filter_clause) > 0:
                filter_string = self.add_filter_clause()
                where_string += '\t'.join(('\n' + filter_string.lstrip()).splitlines(True))
            if len(self.query_model.subqueries) > 0:
                subqueries_string = self.add_subqueries()
                where_string +=  '\t'.join(('\n' + subqueries_string.lstrip()).splitlines(True))
            if len(self.query_model.optionals) > 0:
                optional_string = self.add_optional_clause()
                where_string += '\t'.join(('\n' + optional_string.lstrip()).splitlines(True))
            if len(self.query_model.unions) > 0:
                union_string = self.add_union_query()
                where_string += '\t'.join(('\n' + union_string.lstrip()).splitlines(True))
            if len(self.query_model.optional_subqueries) > 0:
                where_string +=  "\t" + self.add_optional_subqueries()
            if where_string != "":
                where_string = where_string.rstrip('\n')
            return where_string

        def add_where_clause(self):
            """
            prepare where clause of the query
            - adds the triples
            - adds the optional clause if needed
            - adds the filter conditions if needed
            - adds the subqueries if any exist
            """
            if len(self.query_model.triples) > 0 or len(self.query_model.subqueries) > 0 or \
                    len(self.query_model.unions) >0 or len(self.query_model.optionals) > 0 or \
                    len(self.query_model.filter_clause) > 0 or len(self.query_model.optional_subqueries) > 0 or \
                    len(self.query_model.graph_triples) > 0 or len(self.query_model.graph_clause) > 0 or \
                    len(self.query_model.optional_graph_clause) > 0:
                where_string = self.__add_patterns()
                self.query_string += "WHERE {" + where_string + "\n\t}"
            else:
                self.query_string += "WHERE {}"


        def add_union_query(self):
            """

            :return: The string of the main query with the union queries
            """
            unionQuery ="{ "
            if self.query_model.unions is not None:
                for i in range(0,len(self.query_model.unions)):
                    unionQuery += "{\n"\
                                  + self.query_model.unions[i].to_sparql()
                    unionQuery += "}\n"
                    if i < len(self.query_model.unions)-1:
                       unionQuery += "UNION\n"
            return unionQuery+" }"

        def add_order_clause(self):
            """
            append the order specifier is it exists in the query model
            :return:
            """
            if len(self.query_model.order_clause) > 0:
                orderby = " ORDER BY "
                for order_col, sort_order in self.query_model.order_clause.items():
                    orderby += '%s(?%s)' % (sort_order, order_col,) + " "
                orderby += " "
                self.query_string += orderby

        def add_limit(self):
            """
            :return:
            """
            if self.query_model.limit != 0:
                self.query_string += "LIMIT " + str(self.query_model.limit)+"\t"

        def add_offset(self):
            """
            :return:
            """
            if self.query_model.offset != 0:
                self.query_string += "OFFSET " + str(self.query_model.offset) + "\n"
            else:
                return

        def add_groupby(self):
            """
            add group-by clause from the query model into the SPARQL query
            :return:
            """
            if self.query_model.groupBy_columns is not None and len(self.query_model.groupBy_columns) > 0:
                groupby_clause = " GROUP BY "
                for col_name in self.query_model.groupBy_columns:
                    groupby_clause += "?" + col_name + " "
                groupby_clause += "\n"
                self.query_string += groupby_clause

        def add_from(self):
            """
            build From clause from the query model into the SPARQL query
            :return:
            """
            if self.query_model.parent_query_model is None:
                if len(self.query_model.from_clause) > 0:
                    from_clause = "FROM "
                    for graph in self.query_model.from_clause:
                        from_clause += '<{}>, '.format(graph)
                    from_clause = from_clause[:len(from_clause)-2]
                    from_clause += "\n"
                    self.query_string += from_clause

        def add_agg_clause(self):
            """
            :return:
            """
            agg_clause = " "
            for agg_col, agg_part in self.query_model.aggregate_clause.items():
                agg_func = agg_part[0][0]
                src_col_name = agg_part[0][2]
                agg_col_name = agg_col if agg_col is not None else ''
                agg_parameter = agg_part[0][1] if agg_part[0][1] is not None else ''
                agg_clause += " %s (%s ?%s) AS %s" % (agg_func, agg_parameter, src_col_name, agg_col_name)
            self.query_string += agg_clause

        def add_filter_clause(self):
            """
            :return:
            """
            and_clause = " && "
            filter_clause = ""
            cond_list = ""
            if len(self.query_model.filter_clause) > 0:
                filter_clause = "FILTER ( "
                col_i = 0
                for col_name, filter_con in self.query_model.filter_clause.items():
                    col_i += 1
                    if len(filter_con) > 1:
                        for i in range(len(filter_con)):
                            if filter_con[i].find("isIRI") >= 0 or filter_con[0].find("langMatches") >= 0 or filter_con[0].find("regex")  >= 0:
                                cond_string = " ( " + filter_con[i] + " )"
                                if cond_list != "":
                                    cond_list += and_clause
                                    cond_list += cond_string
                                else:
                                    cond_list += cond_string                                
                            elif "date" not in col_name:
                                cond_string = " ( "
                                cond_string += "?%s %s" % (col_name, filter_con[i])
                                cond_string += " )"
                                if cond_list != "":
                                    cond_list += and_clause
                                    cond_list += cond_string
                                else:
                                    cond_list += cond_string
                            else:
                                cond_string = " ("
                                cond_string += "year(xsd:dateTime(?%s)) %s" % (col_name, filter_con[i])
                                cond_string += " )"
                                if cond_list != "":
                                    cond_list += and_clause
                                    cond_list += cond_string
                                else:
                                    cond_list += cond_string
                    else:
                        if filter_con[0].find("isIRI") >= 0 or filter_con[0].find("langMatches") >= 0 or filter_con[0].find("regex")  >= 0:
                            cond_string = filter_con[0]
                            cond_list += cond_string 
                        elif "date" not in col_name:
                            cond_string = " ("
                            cond_string += "?%s %s" % (col_name, filter_con[0])
                            cond_string += " )"
                            cond_list += cond_string
                        else:
                            cond_string = " ("
                            cond_string += "year(xsd:dateTime(?%s)) %s" % (col_name, filter_con[0])
                            cond_string += " )"
                            cond_list += cond_string
                    if col_i < len(self.query_model.filter_clause):
                        cond_list += and_clause

                if filter_clause != "":
                    filter_clause += cond_list + " ) \n"
            return filter_clause

        def add_having_clause(self):
            """
            :return:
            """
            if len(self.query_model.having_clause) > 0:

                having_clause = ""
                cond_list = ""
                if bool(self.query_model.having_clause):
                    having_clause = "HAVING ( "
                for col_name, conditions in self.query_model.having_clause.items():
                    for i in range(len(conditions)):
                        cond_string = "( "
                        func_name, agg_param, src_col_name, condition = conditions[i]
                        agg_param = agg_param if agg_param is not None else ''
                        cond_string += "%s(%s ?%s) %s" % (func_name, agg_param, src_col_name, condition)
                        cond_string += " ) "
                        if cond_list != "":
                            cond_list += " && "
                            cond_list += cond_string
                        else:
                            cond_list += cond_string
                if having_clause != "":
                    having_clause += cond_list + ")"
                self.query_string += having_clause
