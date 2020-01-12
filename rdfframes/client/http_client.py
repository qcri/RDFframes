"""
Http client that handles the execution of the sparql query on the server, retrieves the results and wraps it in
the required format
"""

import requests
import socket
import pandas as pd
from urllib.parse import urlparse
from io import StringIO
import os

from rdfframes.client.client import Client
from rdfframes.utils.constants import _TIMEOUT, ReturnFormat, _MAX_ROWS


class HttpClientDataFormat:
    CSV = "CSV"
    JSON = "JSON"
    TURTLE = "TURTLE"
    HTML = "HTML"
    PANDAS_DF = "PANDAS_DF"
    DEFAULT = CSV

    @staticmethod
    def return_format(comm_format):
        if comm_format == HttpClientDataFormat.CSV or \
           comm_format == HttpClientDataFormat.PANDAS_DF:
            return "text/csv"
        elif comm_format == HttpClientDataFormat.JSON:
            return "application/json"
        elif comm_format == HttpClientDataFormat.HTML:
            return "text/html"
        elif comm_format == HttpClientDataFormat.TURTLE:
            return "application/turtle"
        else:
            return HttpClientDataFormat.return_format(HttpClientDataFormat.DEFAULT)


class HttpClient(Client):
    """
    Submits SPARQL queries via http requests and retrieves the results in the requested format
    """
    def __init__(self,
                 endpoint_url,
                 port=8890,
                 return_format=HttpClientDataFormat.DEFAULT,
                 timeout=120,
                 default_graph_uri='',
                 max_rows=_MAX_ROWS):
        """
        Initializes a client object with the URI of the RDF engine SPARQL endpoint and the port number
        :param endpoint_url: the url of the RDF engine or SPARQL endpoint
        :param port: the endpoint's port number
        :param timeout: the http request timeout in seconds
        :param default_graph_uri: the absolute url of the default graph
        :param max_rows: the maximum number of rows retrieved in a single http request
        :param return_format: the query results format
        """
        super(HttpClient, self).__init__(endpoint_url)

        self.port = None
        self.full_endpoint_url = None
        self.return_format = None
        self.timeout = None
        self.default_graph_uri = None
        self.max_rows = None

        self.set_port(port)
        self.__build_full_url()
        self.set_return_format(return_format)
        self.set_timeout(timeout)
        self.set_graph_uri(default_graph_uri)
        self.set_max_rows(max_rows)

    def set_endpoint(self, endpoint_url):
        """
        setter for the SPARQL endpoint url
        :param endpoint_url: url of the sparql endpoint
        :return: None
        """
        self.endpoint_url = endpoint_url
        self.__endpoint_liveness_check()

    def set_port(self, port):
        """
        setter for the SPARQL endpoint port number
        :param port: port number of the sparql endpoint
        :return: None
        """
        self.port = port
        self.__endpoint_liveness_check()

    def set_return_format(self, return_format=HttpClientDataFormat.CSV):
        """
        setter for the format of the returned data. Options are CSV, JSON, HTML, TURTLE or pandas dataframe (PANDAS_DF)
        :param return_format: return data in format of HttpClientDataFormat
        :return: None
        """
        self.return_format = return_format

    def set_timeout(self, timeout=120):
        """
        setter for the request timeout. Default 120 seconds
        :param timeout: requests timeout period
        :return: None
        """
        self.timeout = timeout

    def set_graph_uri(self, graph_uri):
        """
        setter for the absolute URL of default graph
        :param graph_uri: uri of the default graph
        :return: None
        """
        self.default_graph_uri = graph_uri

    def set_max_rows(self, max_rows):
        """
        setter for the maximum number of rows to return in a single request
        :param max_rows: the maximum number of returned rows in each http request
        :return: None
        """
        if max_rows >= 1:
            self.max_rows = max_rows

    def execute_query(self, query, timeout=_TIMEOUT, limit=_MAX_ROWS, return_format=None, output_file=None):
        """
        submits the provided SPARQL query to the registered endpoint to be executed.
        The result is retrieved in the requested format (return_format)
        :param query: the SPARQL query as string
        :param return_format: the format of the retrieved data. Options from HttpClientDataFormat
        :param export_file: if provided, the data will be saved to the pass file path
        :return: the result of the query in the requested format
        """
        self.return_format = return_format if return_format is not None else self.return_format
        #final_result = None
        final_result = []
        for res in self._execute_query(query, return_format=return_format, export_file=output_file):
            #print('data with type {} and length {} retrieved'.format(type(res).__name__, len(res)))
            if return_format == HttpClientDataFormat.PANDAS_DF:
                #if final_result is None:
                if len(final_result) <= 0:
                    final_result = [res]
                else:
                    final_result.append(res)
                    #final_result = pd.merge(final_result, res, how='outer')
            else:
                raise Exception("return format {} is unimplemented".format(return_format))
        if return_format == HttpClientDataFormat.PANDAS_DF:
            string = ''.join(final_result)
            stringio = StringIO(string)
            stringio.seek(0)
            df = pd.read_csv(stringio, sep=',')
            return df
        else:
            return final_result

    def _execute_query(self, query, return_format=None, export_file=None):
        self.return_format = return_format if return_format is not None else self.return_format
        if HttpClient.__find_clause(query, 'ORDER BY')[0] >= 0:
            self.set_max_rows(1000)


        limit_start, limit_end = HttpClient.__find_clause(query, 'LIMIT')

        query_limit = -1

        if limit_start != -1:
            try:
                query_limit = int(query[limit_start: limit_end])
            except ValueError:
                pass

        offset_start, offset_end = HttpClient.__find_clause(query, 'OFFSET')

        query_offset = 0

        if offset_start != -1:
            try:
                query_offset = int(query[offset_start: offset_end])
            except ValueError:
                pass

        offsets = None

        if limit_start != -1:
            offsets = []

            if offset_start != -1:
                offsets.append(query_offset)
            else:
                offsets.append(0)

            if query_limit > self.max_rows:
                query_limit -= self.max_rows
                current_offset = offsets[0]

                while query_limit > 0:
                    current_offset += self.max_rows
                    query_limit -= self.max_rows
                    offsets.append(current_offset)

        offset_index = 0
        current_offset = -1 * self.max_rows

        first_query_flag = True
        while offsets is None or offset_index < len(offsets):
            modified_query = query
            current_offset = offsets[offset_index] if offsets is not None else current_offset + self.max_rows
            offset_index += 1
            modified_query = HttpClient.__remove_clause(modified_query, 'LIMIT')
            modified_query = HttpClient.__remove_clause(modified_query, 'OFFSET')
            modified_query = HttpClient.__append_clause(modified_query, 'OFFSET', current_offset)
            modified_query = HttpClient.__append_clause(modified_query, 'LIMIT', _MAX_ROWS)
            params = {
                'query': modified_query,
                'format': HttpClientDataFormat.return_format(self.return_format),
                'default-graph-uri': self.default_graph_uri,
                'maxrows': self.max_rows
            }
            response = requests.post(self.full_endpoint_url, data=params, timeout=self.timeout)
            if response.status_code == 200:
                data = self.__handle_http_response(response, export_file, first_query_flag=first_query_flag)
                if data is not None:
                    yield data
                else:
                    break
            else:
                print("HTTP Response is:\n{}".format(response))
                break
            first_query_flag = False

    def __handle_http_response(self, response, export_file, first_query_flag=False):
        """
        Given a valid http response, this methods wraps the response text up in the requested format
        and returns it to the caller
        :param response: http response object
        :param export_file: the file path if the results should be exported to a file
        :return: file name if export to file or the data in the requested format
        """
        data = response.text
        #first_nl = data.find('\n')
        # header = data[:first_nl]
        body = data.split("\n", 1)[1]

        if len(body) > 0:
            if export_file is not None:
                # In order to skip the header line before appending to file
                if os.path.exists(export_file):
                    data = body
                with open(export_file, 'a+') as exp_file:
                    exp_file.write(data)
                    return export_file
            elif self.return_format == HttpClientDataFormat.PANDAS_DF:
                #stringio = StringIO(response.text)
                #return pd.read_csv(stringio, sep=',')
                if first_query_flag:
                    return data
                else:
                    return body
            else:
                return response.text

    def __build_full_url(self):
        """
        if the port number is missing from the url, this method adds it and prepare the full url in one string
        :return: None
        """
        if self.endpoint_url:
            url_comps = urlparse(self.endpoint_url)
            netlocs = url_comps.netloc.split(':')
            netloc = netlocs[0]
            port = netlocs[1] if len(netlocs) > 1 else self.port

            self.full_endpoint_url = '{}://{}:{}{}'.format(url_comps.scheme, netloc, port, url_comps.path)

    @staticmethod
    def __find_clause(query, clause):
        """
        checks if a sparql query already has some clause. Useful when trying to batch retrieve the results
        :param query: the sparql query string
        :return: -1 if the clause does not exist or the range of the number after the clause
        """
        clause = ' {} '.format(clause.strip().lower())
        query = query.lower().replace('\n', ' ')
        pos = query.find(clause)

        if pos == -1:
            return -1, -1
        else:
            i = pos + len(clause)
            while i < len(query) and query[i].isdigit():
                i += 1
            return pos + len(clause), i

    @staticmethod
    def __remove_clause(query, clause):
        """
        if a clause exists in the query, it as well as the associated values are removed
        :param query: the sparql query string
        :param clause: the clause to process such as LIMIT and OFFSET
        :return: the sparql query after removing the clause
        """
        clause = ' {} '.format(clause.strip())
        query = query.strip(' \n;')
        #clause = ' {} '.format(clause.strip().lower())
        #query = query.lower().strip(' \n;')
        start, end = HttpClient.__find_clause(query, clause)
        if start != -1:
            start -= len(clause)
            replace = query[start: end]
            query = query.replace(replace, '')

        return query

    @staticmethod
    def __append_clause(query, clause, value):
        query = query.strip(' ;\n')
        return '{} {} {}'.format(query, clause, value)

    def __endpoint_liveness_check(self):
        """
        checks if the endpoint is alive
        :return: True if alive, False if not
        """
        url_comps = urlparse(self.endpoint_url)
        netloc_comps = url_comps.netloc.split(':')

        url_port = int(netloc_comps[1]) if len(netloc_comps) == 2 else None
        port = url_port if url_port else self.port
        url = netloc_comps[0]

        is_valid = False

        if self.endpoint_url and port:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex((url, port))
            is_valid = (result == 0)

        if not is_valid:
            print('missing endpoint data: endpoint {}, port {}'.format(self.endpoint_url, port))

