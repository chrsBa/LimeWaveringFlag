import os
from pyexpat.errors import messages

import numpy as np
from langchain_ollama import ChatOllama
from numpy import load


class QueryTransformer:
    def __init__(self):
        self.transform_llm = ChatOllama(model="qwen3:30b")

    def transform(self, text_query: str) -> str:
        messages=[
            {"role": "system",
             "content": "You are an assistant that converts natural language questions into SPARQL queries."
                        "Start each query with"
                        "'''"
                        "PREFIX ddis: <http://ddis.ch/atai/> "
                        "PREFIX wd: <http://www.wikidata.org/entity/> "
                        "PREFIX wdt: <http://www.wikidata.org/prop/direct/> "
                        "'''"
                        "Always return the label of the entity using rdfs:label."
                        "Do not add any comments or explanations!"
                        "Do not add any quotes around the query!"
                        "Add normal new lines and indentation!"
                        "DO NOT ADD '''SERVICE''' !"
                        "IMPORTANT: Return only valid SPARQL!"
             },
            {"role": "user", "content": text_query}
        ]
        print("Generating...")
        response = self.transform_llm.invoke(messages)
        return response.content.strip()


if __name__ == "__main__":
    transformer = QueryTransformer()
    query = "Which movie, originally from the country 'South Korea', received the award 'Academy Award for Best Picture'?"
    sparql_query = transformer.transform(query)
    print(sparql_query)

