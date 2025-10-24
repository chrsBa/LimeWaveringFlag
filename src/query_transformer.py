import csv
import os
import re

import numpy as np
import rdflib
from langchain_ollama import ChatOllama

from src.vector_store.vector_store import VectorStore


class QueryTransformer:
    def __init__(self, vector_store: VectorStore):
        self.transform_llm = ChatOllama(model="qwen3:32b")
        self.vector_store = vector_store


    def transform(self, text_query: str) -> str:
        text_query = text_query.strip().replace("?", "")
        estimated_relation_label, estimated_entity_label = self.extract_named_entities(text_query)
        if estimated_relation_label is None or estimated_entity_label is None:
            raise ValueError("Could not extract named entities from the question.")

        node = self.vector_store.label2entity.get(estimated_entity_label)
        pred = self.vector_store.label2entity.get(estimated_relation_label)

        print("Estimated relation label:", estimated_relation_label)
        print("Estimated entity label:", estimated_entity_label)

        if node is None:
            print(self.vector_store.find_similar(estimated_entity_label))
            node = self.vector_store.find_similar(estimated_entity_label)[0]['metadata']['entity']
            print('Could not find', estimated_entity_label, 'most similar is:', node)

        if pred is None:
            pred = self.vector_store.find_similar(estimated_relation_label)[0]['metadata']['entity']
            print(f'Could not find {estimated_relation_label}, most similar is:', pred)

        query = f"""SELECT (COALESCE(?objLabel, STR(?obj)) AS ?result) WHERE {{
                    <{node}> <{pred}> ?obj .
                    OPTIONAL {{
                        ?obj rdfs:label ?objLabel .
                    }}
                }}
                """
        return query


    @staticmethod
    def extract_named_entities(question: str) -> tuple[str, str]:
        print(f"Extracting named entities from question: {question}")
        factual_question_patterns = [
            {
                "pattern": "who is the ([^\\s]+) of (.*)",
                "entity_group_index": 2,
                "relation_group_index": 1,
            },
            {
                "pattern": "who was the ([^\\s]+) of (.*)",
                "entity_group_index": 2,
                "relation_group_index": 1,
            },
            {
                "pattern": "who was the ([^\\s]+) for (.*)",
                "entity_group_index": 2,
                "relation_group_index": 1,
            },
            {
                "pattern": "who was the ([^\\s]+) in (.*)",
                "entity_group_index": 2,
                "relation_group_index": 1,
            },
            {
                "pattern": "who ([^\\s]+) (.*)",
                "entity_group_index": 2,
                "relation_group_index": 1,
            },
            {
                "pattern": "who wrote the ([^\\s]+) of (.*)",
                "entity_group_index": 2,
                "relation_group_index": 1,
            },
            {
                "pattern": "who wrote the ([^\\s]+) for (.*)",
                "entity_group_index": 2,
                "relation_group_index": 1,
            },
            {
                "pattern": "what is the ([^\\s]+) of (.*)",
                "entity_group_index": 2,
                "relation_group_index": 1,
            },
            {
                "pattern": "when was (.*) ([^\\s]+)",
                "entity_group_index": 1,
                "relation_group_index": 2,
            },
            {
                "pattern": "where was (.*) ([^\\s]+)",
                "entity_group_index": 1,
                "relation_group_index": 2,
            },
            {
                "pattern": "where is ([^\\s]+) (.*)",
                "entity_group_index": 1,
                "relation_group_index": 2,
            },
        ]

        for pattern in factual_question_patterns:
            match = re.match(pattern["pattern"], question.rstrip("?"), re.IGNORECASE)

            if match:
                relation = match.group(pattern["relation_group_index"]).strip()
                entity = match.group(pattern["entity_group_index"]).strip()
                return relation, entity

        return None, None


if __name__ == "__main__":
    query_transformer = QueryTransformer(vector_store=VectorStore())
    query = ('Who is the director of Star Wars: Episode VI - Return of the Jedi?')
    result = query_transformer.transform(query)
    print(result)
