import os
import json

class MultimediaSearch:

    def __init__(self, graph_db):
        self.base_path = os.path.dirname(os.path.dirname(__file__))
        self.graph_db = graph_db
        with open(os.path.join(self.base_path, "data", "images.json")) as f:
            self.image_to_entity = json.load(f)

    def find_picture_for_entity(self, extracted_entity):
        try:
            imdb_dict = self.graph_db.get_imdb_id(extracted_entity)
            if imdb_dict is None:
                return None
            imdb_values = next(iter(imdb_dict.values()))
            if len(imdb_values) == 0:
                return None
            imdb_id = imdb_values[0]
            print("finding picture for: " + imdb_id)
            for entry in self.image_to_entity:
                if entry['movie'] == [imdb_id]:
                    return entry['img']
                if entry['cast'] == [imdb_id]:
                    return entry['img']

            return self.image_to_entity[imdb_id]
        except Exception as e:
            print(f"Error finding picture for entity {extracted_entity}: {e}")
            return None
