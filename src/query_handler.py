import os
from rdflib import Graph
from rdflib.plugins.sparql.processor import prepareQuery


class GraphDB:
    def __init__(self):
        self.graph = Graph()
        base_dir = os.path.dirname(__file__)
        graph_path = os.path.join(base_dir, "graph.nt")
        self.graph.parse(graph_path, format="nt")

    def execute_query(self, query: str) -> str:
        """
        Process the sparql queries

        Args:
            query (str): The incoming sparql query to be processed.

        Returns:
            str: The DB response after processing the query.
        """

        prepared_query = prepareQuery(query)
        answer = ""

        for row in self.graph.query(prepared_query):
            answer += str(row[0]) + "\n"

        return answer