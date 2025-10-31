import os

import numpy as np
from sklearn.metrics import pairwise_distances
from rdflib import URIRef


class EmbeddingSearch:
    def __init__(self, vector_store):
        self.vector_store = vector_store
        self.base_path = os.path.dirname(os.path.dirname(__file__))
        self.entity_embeddings = np.load(os.path.join(self.base_path, 'data', 'entity_embeds.npy'))
        self.relation_embeddings = np.load(os.path.join(self.base_path, 'data', 'relation_embeds.npy'))
        self.entity2id, self.relation2id = self.load_mappings()

    def load_mappings(self):
        entity2id = {}
        relation2id = {}

        with open(os.path.join(self.base_path, 'data', 'relation_ids.del')) as f:
            for line in f:
                idx, name = line.strip().split()
                relation2id[name] = int(idx)

        with open(os.path.join(self.base_path, 'data', 'entity_ids.del')) as f:
            for line in f:
                idx, name = line.strip().split()
                entity2id[name] = int(idx)

        return entity2id, relation2id

    def nearest_neighbor(self, entity_uri, relation_uri) -> str:
        try:
            relation_vector = self.relation_embeddings[self.relation2id[relation_uri]]
            entity_vector = self.entity_embeddings[self.entity2id[entity_uri]]

            target = np.atleast_2d(entity_vector+relation_vector)

            distances = pairwise_distances(target, self.entity_embeddings)

            top_index = np.argmin(distances)

            id2entity = {idx: name for name, idx in self.entity2id.items()}
            top_entity = id2entity[top_index]

            print(top_entity)
            return self.vector_store.entity2label.get(URIRef(top_entity))
        except Exception as e:
            return None