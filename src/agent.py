from speakeasypy import Chatroom, EventType, Speakeasy

from cred import USERNAME, PASSWORD
from src.message_handler import MessageHandler
import re

from transformer import Transformer
from vector_store.vector_store import VectorStore

DEFAULT_HOST_URL = 'https://speakeasy.ifi.uzh.ch'


class Agent:
    def __init__(self, username: str, password: str):
        self.username = username
        # Initialize the Speakeasy Python framework and login.
        self.speakeasy = Speakeasy(host=DEFAULT_HOST_URL, username=username, password=password)
        self.speakeasy.login()  # This framework will help you log out automatically when the program terminates.

        self.speakeasy.register_callback(self.on_new_message, EventType.MESSAGE)
        self.speakeasy.register_callback(self.on_new_reaction, EventType.REACTION)
        self.speakeasy.register_callback(self.on_new_room, EventType.ROOMS)

        self.vector_store = VectorStore()
        self.message_handler = MessageHandler(self.vector_store)
        self.transformer = Transformer(self.vector_store)
        self.cached_responses = {}

    def listen(self):
        """Start listening for events."""
        self.speakeasy.start_listening()

    def on_new_room(self, room : Chatroom):
        """Callback function to handle new rooms."""
        room.post_messages("Hey!\n I'm your Movie Knowledge Bot🤖. Ask me anything about movies🎥!")

    def on_new_message(self, message : str, room : Chatroom):
        """Callback function to handle new messages."""
        try:
            if message in self.cached_responses:
                room.post_messages(self.cached_responses[message])
                return

            room.post_messages("Let's have a look...")
            suggestion_response_needed = False
            factual_answer_needed = False
            embedding_answer_needed = False
            multimedia_answer_needed = False

            if re.search(r"\b(show|pictures?|photos?|posters?|images?|looks?)\b", message, re.IGNORECASE):
                multimedia_answer_needed = True
                extracted_m_entity = self.transformer.extract_multimedia_entity(message)
                print("extracted media entity: " + extracted_m_entity)

            elif re.search(r"\b(recommend|similar|like|suggest|suggestions?)\b", message, re.IGNORECASE):
                suggestion_response_needed = True
                extracted_entities_map = self.transformer.extract_suggestion_entities(message)
                print("extracted suggestion entities " + str(extracted_entities_map.keys()))
            else:
                extracted_relation, extracted_entity = self.transformer.extract_movie_relation_entities(message)
                print("extracted entity " + extracted_entity)
                print("extracted relation " + extracted_relation)
                factual_answer_needed = True
                text_query = message

            response = ""
            if factual_answer_needed:
                response = self.message_handler.handle_factual_question(text_query, extracted_entity, extracted_relation)
                print("factual response: " + response)
                room.post_messages(response)
            if embedding_answer_needed:
                response = self.message_handler.handle_embedding_question(text_query, extracted_entity, extracted_relation)
                print("embedding response: " + response)
                room.post_messages(response)
            if suggestion_response_needed:
                response = self.message_handler.handle_suggestion_question(message, extracted_entities_map)
                print("suggestion response: " + response)
                room.post_messages(response)
            if multimedia_answer_needed:
                response = self.message_handler.handle_multimedia_question(extracted_m_entity)
                room.post_messages(response)

            self.cached_responses[message] = response

        except Exception as e:
            print(e)
            room.post_messages("Sorry, I could not understand your question. Please try rephrasing it.")
            return

    def on_new_reaction(self, reaction : str, message_ordinal : int, room : Chatroom):
        print(reaction)
        """Callback function to handle new reactions."""
        pass


if __name__ == '__main__':
    agent = Agent(USERNAME, PASSWORD)
    print('Agent is listening for messages...')
    agent.listen()