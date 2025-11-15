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

    def extract_movies(self):
        relevant_types = [
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q11424'), #'film'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q17123180'), #'sequel film'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q202866'), #'animated film'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q622548'), #'parody film'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q622548'), #'parody film'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q10590726'), #'video album'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q917641'), #'open-source film'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q52207399'), #'film based on a novel'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q31235'), #'remake'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q24862'), #'short film'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q104840802'), #'film remake'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q112158242'), #'Tom and Jerry film'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q24856'), #'film series'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q117467246'), #'animated television series'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q2484376'), #'thriller film',
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q20650540'), #'anime film',
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q13593818'), #'film trilogy'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q17517379'), #'animated short film'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q678345'), #'prequel'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q1257444'), #'film adaptation'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q52162262'), #'film based on literature'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q118189123'), #'animated film reboot'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q1259759'), #'miniseries'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q506240'), #'television film'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q5398426'),  # 'television series'
        ]

        relevant_properties = [
            rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P31"),  # instance of
            rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P57"),  # director
            rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P58"),  # screenwriter
            rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P577"),  # release date
            rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P136"),  # genre
            rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P166"),  # award received
            rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P921"),  # main subject
            rdflib.term.URIRef("http://www.wikidata.org/prop/direct/495"),  # country of origin
            rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P272"),  # production company
        ]
        relevant_entities = {}
        entity2label = {}

        for s, p, o in self.graph:
            if p == rdflib.term.URIRef("http://www.w3.org/2000/01/rdf-schema#label"):
                entity2label[s] = str(o)
        for s, p, o in self.graph:
            if p == rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P31") and o in relevant_types:
                if s not in relevant_entities:
                    relevant_entities[s] = {
                        "instance_of":[entity2label.get(o, str(o))],
                        "director": [],
                        "screenwriter": [],
                        "publication_date": None,
                        "genre": [],
                        "award_received": [],
                        "main_subject": [],
                        "country_of_origin": [],
                        "production_company": [],
                    }
                else:
                    relevant_entities[s]["instance_of"].append(entity2label.get(o, str(o)))

        for s, p, o in self.graph:
            if s in relevant_entities and p in relevant_properties:
                if p == rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P57"):
                    relevant_entities[s]["director"].append(entity2label.get(o, str(o)))
                elif p == rdflib.term.URIRef("http://www.w3.org/2000/01/rdf-schema#label"):
                    relevant_entities[s].append(entity2label.get(o, str(o)))
                elif p == rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P58"):
                    relevant_entities[s]["screenwriter"].append(entity2label.get(o, str(o)))
                elif p == rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P577"):
                    relevant_entities[s]["publication_date"] = str(o)
                elif p == rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P136"):
                    relevant_entities[s]["genre"].append(entity2label.get(o, str(o)))
                elif p == rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P166"):
                    relevant_entities[s]["award_received"].append(entity2label.get(o, str(o)))
                elif p == rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P921"):
                    relevant_entities[s]["main_subject"].append(entity2label.get(o, str(o)))
                elif p == rdflib.term.URIRef("http://www.wikidata.org/prop/direct/495"):
                    relevant_entities[s]["country_of_origin"].append(entity2label.get(o, str(o)))
                elif p == rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P272"):
                    relevant_entities[s]["production_company"].append(entity2label.get(o, str(o)))

        # Save to CSV
        src_dir = os.path.dirname(__file__)
        base_dir = os.path.dirname(src_dir)
        csv_path = os.path.join(base_dir, "data", "movies_with_properties.csv")
        with open(csv_path, mode="w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            for entity, properties in relevant_entities.items():
                row = [
                    entity2label.get(entity, ""),
                    entity,
                    "|".join(properties["instance_of"]),
                    "|".join(properties["director"]),
                    "|".join(properties["screenwriter"]),
                    "|".join(properties["genre"]),
                    "|".join(properties["award_received"]),
                    "|".join(properties["main_subject"]),
                    "|".join(properties["country_of_origin"]),
                    "|".join(properties["production_company"]),
                    properties["publication_date"],
                ]
                writer.writerow(row)

    def get_entity_type(self, uri: str):
        query = f"""
            SELECT ?type WHERE {{
            <{uri}> <http://www.wikidata.org/prop/direct/P31> ?type .
            }}
        """
        answer = self.execute_query(query)

        try:
            return answer.split("entity/")[1]
        except:
            return "Unknown"

    def get_movie_properties(self, uri: str):
        def get_property(property_uri: str):
            query = f"""
                SELECT (COALESCE(?objLabel, STR(?obj)) AS ?result) WHERE {{
                    <{uri}> <{property_uri}> ?obj .
                    OPTIONAL {{
                        ?obj rdfs:label ?objLabel .
                    }}
                }}
            """
            answer = self.execute_query(query)
            if answer.strip() == "":
                return []
            return [line.strip() for line in answer.strip().split("\n")]

        properties = {
            "instance_of": "http://www.wikidata.org/prop/direct/P31",
            "publication_date": "http://www.wikidata.org/prop/direct/P577",
            "genre": "http://www.wikidata.org/prop/direct/P136",
            "award_received": "http://www.wikidata.org/prop/direct/P166",
            "main_subject": "http://www.wikidata.org/prop/direct/P921",
            "country_of_origin": "http://www.wikidata.org/prop/direct/495",
            "production_company": "http://www.wikidata.org/prop/direct/P272",
        }

        result = {}

        for property_name, property_uri in properties.items():
            result[property_name] = get_property(property_uri)

        return result




if __name__ == "__main__":
    graph_db = GraphDB()
    graph_db.extract_entities()
    graph_db.extract_movies()