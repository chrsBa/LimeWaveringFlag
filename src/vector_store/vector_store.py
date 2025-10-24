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

from src.vector_store.batch_inserter import BatchInserter
from src.vector_store.table_schema import TableSchema


class VectorStore:
    def __init__(self):
        self.vector_db_path = '../data/lancedb'
        self.entities_table_name = 'entities'
        self.vector_db = lancedb.connect(self.vector_db_path)
        self.entities_table = self._instantiate_table(self.entities_table_name)
        print(self.entities_table.stats())
        self.batcher = BatchInserter(self.entities_table)

        self.logger = logging.getLogger(__name__)

        self.entity2label = {}
        self.label2entity = {}
        self._load_entity_label_mapping()

    def find_similar(self, estimated_label: str, k=1) -> List[dict]:
        print(self.entities_table.search(query=estimated_label).rerank(CrossEncoderReranker("cross-encoder/ms-marco-MiniLM-L12-v2")).limit(k).to_list())
        return self.entities_table.search(query=estimated_label).rerank(CrossEncoderReranker("cross-encoder/ms-marco-MiniLM-L12-v2")).limit(k).to_list()

    def fill_vector_store(self):
        self.batcher.start()
        with ThreadPoolExecutor(max_workers=20) as executor:
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
            document = Document(
                id=uuid.uuid4().hex,
                page_content=entity_label,
                metadata={
                    "entity": entity_uri,
                    "label": entity_label
                    }
                )
            self.batcher.add_document(document)
        except Exception as e:
            self.logger.error(f"Error processing entity {entity_label}: {e}")

    def _compute_hash(self, text):
        return xxhash.xxh64(text).hexdigest()

    def _load_entity_label_mapping(self):
        with open(os.path.join('..', 'data', 'entities.csv'), 'r') as csv_file:
            self.entity2label = {rdflib.term.URIRef(ent): label for ent, label in csv.reader(csv_file)}
            self.label2entity = {v: k for k, v in self.entity2label.items()}

    def _instantiate_table(self, table_name: str):
        try:
            return self.vector_db.open_table(table_name)
        except:
            print("Creating new vector store table: ", table_name)
            return self.vector_db.create_table(table_name, schema=TableSchema, mode="overwrite")


if __name__ == "__main__":
    vector_store = VectorStore()
    vector_store.fill_vector_store()