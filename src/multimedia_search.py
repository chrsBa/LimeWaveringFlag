import os

import numpy as np
import json

class MultimediaSearch:

    def __init__(self, vector_store):
        self.vector_store = vector_store
        self.base_path = os.path.dirname(os.path.dirname(__file__))
        with open(os.path.join(self.base_path, "data", "images.json")) as f:
            self.image_to_entity = json.load(f)

    def find_picture(self, extracted_id):
        print("finding picture for: " + extracted_id)
        try:
            for entry in self.image_to_entity:
                if entry['movie'] == [extracted_id]:
                    return entry['img']
                if entry['cast'] == [extracted_id]:
                    return entry['img']

            return self.image_to_entity[extracted_id]
        except:
            return extracted_id
