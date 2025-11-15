import csv
import logging
import os
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

import lancedb
import rdflib
import xxhash
from lancedb.rerankers import CrossEncoderReranker
from langchain_core.documents import Document
from tqdm import tqdm

from .batch_inserter import BatchInserter
from .table_schema import TableSchema


class VectorStore:
    def __init__(self):
        base_dir = os.path.dirname(__file__)
        self.vector_db_path = os.path.join(base_dir, '..', '..', 'data', 'lancedb')
        self.entities_table_name = 'entities'
        self.movies_properties_table_name = 'movies_properties'
        self.movies_labels_table_name = 'movie_labels'
        self.vector_db = lancedb.connect(self.vector_db_path)
        self.entities_table = self._instantiate_table(self.entities_table_name)
        self.movie_properties_table = self._instantiate_table(self.movies_properties_table_name)
        self.movie_labels_table = self._instantiate_table(self.movies_labels_table_name)

        self.logger = logging.getLogger(__name__)

        self.synonyms = {
                "award": ["award",
                          "oscar",
                          "prize"],
                "publication date": [
                    "release",
                    "date",
                    "released",
                    "releases",
                    "release date",
                    "release_date"
                    "publication",
                    "launch",
                    "broadcast",
                    "launched",
                    "come out",
                ],
                "executive producer": ["showrunner",
                                       "executive producer"],
                "screenwriter": ["screenwriter",
                                 "scriptwriter",
                                 "writer",
                                 "story"],
                "film editor": ["editor",
                                "film editor"],
                "country of origin": ["origin", "country", "country of origin"],
                "director": [
                    "directed",
                    "director",
                    "directs",
                    "direct"],
                "nominated for": [
                    "nomination",
                    "award",
                    "finalist",
                    "shortlist",
                    "selection",
                    "nominated for",
                ],
                "cost": ["budget",
                         "cost"],
                "production company": [
                    "company",
                    "company of production",
                    "produced",
                    "production company",
                ],
                "cast member": [
                                "cast member",
                                "actor",
                                "actress",
                                "cast"],
                "genre": ["kind",
                          "type",
                          "genre"],
            }

        self.entity2label = {}
        self.label2entity = {}
        self.entity2description = {}
        self._load_entity_label_mapping()

    def find_similar_entity(self, estimated_label: str, k=1) -> List[dict]:
        print("entity estimate: " + estimated_label)
        return (self.entities_table.search(query=estimated_label)
                .where("metadata.type = 'entity' AND (LOWER(metadata.description) LIKE '%movie%' OR LOWER(metadata.description) LIKE '%film%')")
                .rerank(CrossEncoderReranker("cross-encoder/ms-marco-MiniLM-L12-v2")).limit(k).to_list())

    def find_similar_relation(self, estimated_label: str, k=1) -> List[dict]:
        print("relation estimate: " + estimated_label)
        return (self.entities_table.search(query=estimated_label)
                .where(f"metadata.type = 'relation' AND (LOWER(metadata.description) LIKE '%movie%' OR LOWER(metadata.description) LIKE '%film%')")
                .rerank(CrossEncoderReranker("cross-encoder/ms-marco-MiniLM-L12-v2")).limit(k).to_list())

    def find_movie_with_label(self, label: str) -> List[dict]:
        return (self.movie_labels_table.search(query=label)
                .limit(1).to_list())

    def find_similar_movies(self, movie_properties: str, k=5) -> List[dict]:
        return (self.movie_properties_table.search(query=movie_properties)
                .limit(k).to_list())

    def fill_entity_vector_store(self):
        batcher = BatchInserter(self.entities_table)
        batcher.start()
        with ThreadPoolExecutor(max_workers=12) as executor:
            futures = {
                executor.submit(self._process_entities, label, entity, batcher): label
                for label, entity in self.label2entity.items()
            }
            for future in tqdm(as_completed(futures), total=len(futures), desc="Embedding Entities"):
                label = futures[future]
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Error while processing entity {label}: {e}")

        batcher.finish()
        self.logger.debug("Vectorization complete.")

    def fill_movie_properties_vector_store(self):
        batcher = BatchInserter(self.movie_properties_table, batch_size=200)
        batcher.start()

        vect_dir = os.path.dirname(__file__)
        src_dir = os.path.dirname(vect_dir)
        base_dir = os.path.dirname(src_dir)
        movies = {}
        with open(os.path.join(base_dir, 'data', 'movies_with_properties.csv'), 'r', encoding="utf-8") as csv_file:
            rows = csv.reader(csv_file)
            for row in rows:
                # Create a string for each movie with its properties
                movies[row[1]] = {
                    "label": row[0],
                    "properties": ', '.join([str(row[2]), str(row[3]), str(row[4]), str(row[5]), str(row[6]), str(row[7])])
                }


        with ThreadPoolExecutor(max_workers=12) as executor:
            futures = {
                executor.submit(self._process_movie_properties, entity, data["properties"], data["label"], batcher): entity
                for entity, data in movies.items()
            }
            for future in tqdm(as_completed(futures), total=len(futures), desc="Embedding Entities"):
                entity = futures[future]
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Error while processing entity {entity}: {e}")

        batcher.finish()
        self.logger.debug("Vectorization complete.")

    def fill_movie_labels_vector_store(self):
        batcher = BatchInserter(self.movie_labels_table, batch_size=200)
        batcher.start()

        vect_dir = os.path.dirname(__file__)
        src_dir = os.path.dirname(vect_dir)
        base_dir = os.path.dirname(src_dir)
        movies = {}
        with open(os.path.join(base_dir, 'data', 'movies_with_properties.csv'), 'r', encoding="utf-8") as csv_file:
            rows = csv.reader(csv_file)
            for row in rows:
                # Create a string for each movie with its properties
                movies[row[1]] = row[0]

        with ThreadPoolExecutor(max_workers=12) as executor:
            futures = {
                executor.submit(self._process_movie_labels, entity, label, batcher): entity
                for entity, label in movies.items()
            }
            for future in tqdm(as_completed(futures), total=len(futures), desc="Embedding Movies"):
                entity = futures[future]
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Error while processing entity {entity}: {e}")

        batcher.finish()
        self.logger.debug("Vectorization complete.")

    def _process_entities(self, entity_label: str, entity_uri: str, batcher: BatchInserter):
        try:
            entity_code = entity_uri.split('/')[-1]

            if entity_code.startswith('P'):
                entity_type = 'relation'
            else:
                entity_type = 'entity'

            description = self.entity2description.get(entity_uri, "")
            if entity_type == 'relation':
                content_to_vectorize = f"{entity_label}: {description}"
            else:
                content_to_vectorize = entity_label

            document = Document(
                id=uuid.uuid4().hex,
                page_content=content_to_vectorize,
                metadata={
                    "entity": entity_uri,
                    "label": entity_label,
                    "description": description,
                    "type": entity_type,
                    }
                )
            batcher.add_document(document)
        except Exception as e:
            self.logger.error(f"Error processing entity {entity_label}: {e}")

    def _process_movie_properties(self, entity_uri: str, properties: str, label: str, batcher: BatchInserter):
        try:
            document = Document(
                id=uuid.uuid4().hex,
                page_content=properties,
                metadata={
                    "entity": entity_uri,
                    "label": label,
                    "description": properties,
                    "type": '',
                }
            )
            batcher.add_document(document)
        except Exception as e:
            self.logger.error(f"Error processing entity {entity_uri}: {e}")

    def _process_movie_labels(self, entity_uri: str, label: str, batcher: BatchInserter):
        try:
            document = Document(
                id=uuid.uuid4().hex,
                page_content=label,
                metadata={
                    "entity": entity_uri,
                    "label": label,
                    "description": '',
                    "type": '',
                }
            )
            batcher.add_document(document)
        except Exception as e:
            self.logger.error(f"Error processing entity {entity_uri}: {e}")

    def _compute_hash(self, text):
        return xxhash.xxh64(text).hexdigest()

    def _load_entity_label_mapping(self):
        vect_dir = os.path.dirname(__file__)
        src_dir = os.path.dirname(vect_dir)
        base_dir = os.path.dirname(src_dir)
        with open(os.path.join(base_dir, 'data', 'entities.csv'), 'r', encoding="utf-8") as csv_file:
            self.entity2label = {rdflib.term.URIRef(ent): label for ent, label in csv.reader(csv_file)}
            self.label2entity = {v: k for k, v in self.entity2label.items()}

        with open(os.path.join(base_dir, 'data', 'descriptions.csv'), 'r', encoding="utf-8") as csv_file:
            self.entity2description = {rdflib.term.URIRef(ent): descr for ent, descr in csv.reader(csv_file)}

        for label, synonyms in self.synonyms.items():
            if label in self.label2entity:
                entity_uri = self.label2entity[label]
                self.entity2description[rdflib.term.URIRef(entity_uri)] = (
                        self.entity2description.get(rdflib.term.URIRef(entity_uri), "") + " (movie)")
                for synonym in synonyms:
                    self.label2entity[synonym] = rdflib.term.URIRef(entity_uri)

    def _instantiate_table(self, table_name: str):
        try:
            return self.vector_db.open_table(table_name)
        except Exception as e:
            return self.vector_db.create_table(table_name, schema=TableSchema, mode="overwrite")


if __name__ == "__main__":
    vector_store = VectorStore()
    vector_store.fill_movie_labels_vector_store()
    vector_store.fill_movie_properties_vector_store()
    vector_store.fill_entity_vector_store()