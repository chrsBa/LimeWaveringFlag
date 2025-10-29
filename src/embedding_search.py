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


    def nearest_neighbor(self, entity_uri, relation_uri) -> str:
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


        relation_vector = self.relation_embeddings[relation2id[relation_uri]]
        entity_vector = self.entity_embeddings[entity2id[entity_uri]]

        target = entity_vector+relation_vector

        distances = pairwise_distances(target.reshape(1, -1), self.entity_embeddings, metric='cosine').flatten()

        top_index = np.argmin(distances)

        id2entity = {idx: name for name, idx in entity2id.items()}
        top_entity = id2entity[top_index]
        
        return self.vector_store.entity2label.get(URIRef(top_entity))