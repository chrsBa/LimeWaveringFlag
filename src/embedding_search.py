import os

import numpy as np


class EmbeddingSearch:
    def __init__(self, vector_store):
        self.vector_store = vector_store
        base_path = os.path.dirname(__file__)
        self.entity_embeddings = np.load(os.path.join(base_path,'..', 'data', 'entity_embeds.npy'))
        self.relation_embeddings = np.load(os.path.join(base_path,'..', 'data', 'relation_embeds.npy'))


    def nearest_neighbor(self, entity_uri, relation_uri) -> str:
        return ''
