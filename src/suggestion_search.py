import os

import numpy as np
from sklearn.metrics import pairwise_distances
from rdflib import URIRef


class SuggestionSearch:
    def __init__(self, vector_store, embedding_search):
        self.vector_store = vector_store
        self.embedding_search = embedding_search
        self.base_path = os.path.dirname(os.path.dirname(__file__))
        self.entity_embeddings = np.load(os.path.join(self.base_path, 'data', 'entity_embeds.npy'))
        self.relation_embeddings = np.load(os.path.join(self.base_path, 'data', 'relation_embeds.npy'))
        self.entity2id, self.relation2id = self.embedding_search.load_mappings()


    def find_suggestions(self, entity_uris: list[str]) -> str:
        try:
            entity_vectors = [self.entity_embeddings[self.entity2id[entity_uri]] for entity_uri in entity_uris]
            print(entity_vectors)

            target = np.atleast_2d(np.mean(entity_vectors)) 

            distances = pairwise_distances(target, self.entity_embeddings)
            top_index = np.argsort(distances[0])[:5]

            id2entity = {idx: name for name, idx in self.entity2id.items()}
            top_entities = [id2entity[idx] for idx in top_index]

            print("top suggested entities: ")
            print(top_entities)

            entity_names = [self.vector_store.entity2label.get(URIRef(entity)) for entity in top_entities]

            print(entity_names)
            return entity_names
        
        except Exception as e:
            return None