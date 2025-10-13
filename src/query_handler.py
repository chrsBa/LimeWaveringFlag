import os
from rdflib import Graph

g = Graph()
if len(g) == 0:
    base_dir = os.path.dirname(__file__)
    graph_path = os.path.join(base_dir, "graph.nt")
    g.parse(graph_path, format="nt")

def handle_query(query: str) -> str:
    """
    Process the sparql queries

    Args:
        query (str): The incoming sparql query to be processed.

    Returns:
        str: The DB response after processing the query.
    """

    answer = ""

    for row in g.query(query):
        answer += str(row[0]) + "\n"

    return answer