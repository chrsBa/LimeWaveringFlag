import os
import csv

import re
import rdflib
from rdflib import Graph, RDFS, URIRef
from rdflib.plugins.sparql.processor import prepareQuery



class GraphDB:
    def __init__(self):
        self.graph = Graph()
        src_dir = os.path.dirname(__file__)
        base_dir = os.path.dirname(src_dir)
        graph_path = os.path.join(base_dir, "data", "graph.nt")
        self.relevant_suggestion_properties = {
            "instance_of": rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P31"),
            "publication_date": rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P577"),
            "director": rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P57"),
            "genre": rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P136"),
            "award_received": rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P166"),
            "main_subject": rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P921"),
            "production_company": rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P272"),
            "after_a_work_by": rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P1877"),
            "narrative_location": rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P840"),
            "fsk_rating": rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P1981"),
            "composer": rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P86"),
            "producer": rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P162"),
            "director_of_photography": rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P344"),
            "screenwriter": rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P58"),
            "film_editor": rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P1040"),
            "nominated_for": rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P1411"),
            "sound_designer": rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P5028"),
            "movement": rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P135"),
        }
        print('Loading Graph...')
        self.graph.parse(graph_path, format="nt")
        print('Successfully loaded Graph.')
        self.lbl2ent = {str(lbl): ent for ent, lbl in self.graph.subject_objects(RDFS.label)}

    def execute_query(self, query: str, separator=" and ") -> str:
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
                        answer += separator
            else:
                answer += str(row[0]) + separator

        return answer.rstrip(separator)

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
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q11424'),  # 'film'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q17123180'),  # 'sequel film'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q202866'),  # 'animated film'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q622548'),  # 'parody film'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q917641'),  # 'open-source film'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q52207399'),  # 'film based on a novel'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q31235'),  # 'remake'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q24862'),  # 'short film'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q104840802'),  # 'film remake'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q112158242'),  # 'Tom and Jerry film'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q24856'),  # 'film series'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q2484376'),  # 'thriller film',
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q20650540'),  # 'anime film',
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q13593818'),  # 'film trilogy'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q17517379'),  # 'animated short film'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q678345'),  # 'prequel'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q1257444'),  # 'film adaptation'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q52162262'),  # 'film based on literature'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q118189123'),  # 'animated film reboot'
            rdflib.term.URIRef('http://www.wikidata.org/entity/Q506240'),  # 'television film'
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
                        "publication_date": None,
                        "genre": [],
                        "award_received": [],
                        "main_subject": [],
                        "production_company": [],
                        "after_a_work_by": [],
                        "narrative_location": [],
                        "fsk_rating": [],
                        "composer": [],
                        "producer": [],
                        "director_of_photography": [],
                        "screenwriter": [],
                        "film_editor": [],
                        "nominated_for": [],
                        "sound_designer": [],
                        "movement": [],
                    }
                else:
                    relevant_entities[s]["instance_of"].append(entity2label.get(o, str(o)))

        for s, p, o in self.graph:
            if s in relevant_entities and p in self.relevant_suggestion_properties.values():
                for key, value in self.relevant_suggestion_properties.items():
                    if p == value:
                        if key == "publication_date":
                            relevant_entities[s][key] = str(o)
                        else:
                            relevant_entities[s][key].append(entity2label.get(o, str(o)))

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
                    properties["publication_date"],
                ]
                for key, values in properties.items():
                    if key in ["instance_of", "publication_date"]:
                        continue
                    row.append("; ".join(values))
                writer.writerow(row)

        relevant_general_properties = [
            rdflib.term.URIRef("http://www.wikidata.org/prop/direct/P136"),
        ]
        relevant_general_property_keywords = []
        for s, p, o in self.graph:
            if s in relevant_entities and p in relevant_general_properties:
                relevant_general_property_keywords.append(entity2label.get(o, str(o)))
        relevant_general_property_keywords = set(relevant_general_property_keywords)

        keywords_csv_path = os.path.join(base_dir, "data", "movie_general_property_keywords.csv")
        with open(keywords_csv_path, mode="w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            for keyword in relevant_general_property_keywords:
                cleaned_keyword = keyword.replace("film", "").replace("movie", "").strip()
                if len(cleaned_keyword) > 1:
                    writer.writerow([cleaned_keyword])
                    writer.writerow([keyword])

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
            answer = self.execute_query(query, separator="\n")
            if answer.strip() == "":
                return []
            return [line.strip() for line in answer.strip().split("\n")]

        result = {}

        for property_name, property_uri in self.relevant_suggestion_properties.items():
            result[property_name] = get_property(property_uri)

        return result
    
    def get_imdb_id(self, uri: str):
        imdb_property = URIRef("http://www.wikidata.org/prop/direct/P345")  

        title_pattern = re.compile(r"tt\d{7,}")
        name_pattern = re.compile(r"nm\d{7,}")
        entity = URIRef(uri)
        result = {}

        for _, _, o in self.graph.triples((entity, imdb_property, None)):
            imdb_id = str(o)
            if title_pattern.match(imdb_id):
                result.setdefault("movies", []).append(imdb_id)
            elif name_pattern.match(imdb_id):
                result.setdefault("actors", []).append(imdb_id)

        return result




if __name__ == "__main__":
    graph_db = GraphDB()
    graph_db.extract_entities()
    graph_db.extract_movies()