import os
import csv
from rdflib import Graph
from rdflib.plugins.sparql.processor import prepareQuery



class GraphDB:
    def __init__(self):
        self.graph = Graph()
        src_dir = os.path.dirname(__file__)
        base_dir = os.path.dirname(src_dir)
        graph_path = os.path.join(base_dir, "data", "graph.nt")
        print('Loading Graph...')
        self.graph.parse(graph_path, format="nt")
        print('Successfully loaded Graph.')

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

    def extract_entities(self):
        # Extract entities and their labels from the graph
        ent2lbl = {ent: str(lbl) for ent, lbl in self.graph.subject_objects()}
        lbl2ent = {lbl: ent for ent, lbl in ent2lbl.items()}

        # Save to CSV
        src_dir = os.path.dirname(__file__)
        base_dir = os.path.dirname(src_dir)
        csv_path = os.path.join(base_dir, "data", "entities.csv")
        with open(csv_path, mode="w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            for entity, label in ent2lbl.items():
                writer.writerow([entity, label])




if __name__ == "__main__":
    graph_db = GraphDB()
    graph_db.extract_entities()