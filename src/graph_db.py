import os
import csv

import rdflib
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
            if len(row) > 1:
                for index, item in enumerate(row):
                    answer += str(item)
                    if index < len(row) - 1:
                        answer += " and "
            else:
                answer += str(row[0]) + "\n"

        return answer

    def extract_entities(self):
        # Extract entities and their labels from the graph
        entity2descriptions = {}
        entity2label = {}
        for s, p, o in self.graph:
            if p == rdflib.term.URIRef("http://www.w3.org/2000/01/rdf-schema#label"):
                entity2label[s] = str(o)
            if p == rdflib.term.URIRef("http://schema.org/description"):
                entity2descriptions[s] = str(o)

        # Save to CSV
        src_dir = os.path.dirname(__file__)
        base_dir = os.path.dirname(src_dir)
        csv_path = os.path.join(base_dir, "data", "entities.csv")

        descr_csv_path = os.path.join(base_dir, "data", "descriptions.csv")
        with open(descr_csv_path, mode="w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            for entity, descr in entity2descriptions.items():
                writer.writerow([entity, descr])

        with open(csv_path, mode="w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            for entity, label in entity2label.items():
                print(entity, label)
                writer.writerow([entity, label])

    def get_entity_type(self, uri: str):
        query = f"""
            SELECT ?type WHERE {{
            <{uri}> <http://www.wikidata.org/prop/direct/P31> ?type .
            }}
        """
        answer = self.execute_query(query)

        return answer.split("entity/")[1]





if __name__ == "__main__":
    graph_db = GraphDB()
    graph_db.extract_entities()