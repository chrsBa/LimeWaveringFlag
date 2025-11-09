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
        self.vector_db = lancedb.connect(self.vector_db_path)
        self.entities_table = self._instantiate_table(self.entities_table_name)
        self.batcher = BatchInserter(self.entities_table)

        self.logger = logging.getLogger(__name__)

        self.synonyms = synonyms = {
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

    def fill_vector_store(self):
        self.batcher.start()
        with ThreadPoolExecutor(max_workers=12) as executor:
            futures = {
                executor.submit(self._process_entities, label, entity): label
                for label, entity in self.label2entity.items()
            }
            for future in tqdm(as_completed(futures), total=len(futures), desc="Embedding Entities"):
                label = futures[future]
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Error while processing entity {label}: {e}")

        self.batcher.finish()
        self.logger.debug("Vectorization complete.")

    def _process_entities(self, entity_label: str, entity_uri: str):
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
            self.batcher.add_document(document)
        except Exception as e:
            self.logger.error(f"Error processing entity {entity_label}: {e}")

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
    vector_store.fill_vector_store()

    # print(vector_store.entities_table.stats())
    # similar_items = vector_store.find_similar_relation('Who directed the movie G.I. Joe', k=5)
    # for similar_item in similar_items:
    #     print(similar_item['metadata'])