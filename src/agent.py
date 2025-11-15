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

        self.vector_store = VectorStore()
        self.message_handler = MessageHandler(self.vector_store)
        self.transformer = Transformer(self.vector_store)

    def listen(self):
        """Start listening for events."""
        self.speakeasy.start_listening()

    def on_new_message(self, message : str, room : Chatroom):
        """Callback function to handle new messages."""
        try:
            room.post_messages("Let's have a look...")
            suggestion_response_needed = False
            factual_answer_needed = False
            embedding_answer_needed = False

            if re.search(r"\brecommend\b|\bsimilar\b|\blike\b|\bsuggestions?\b", message, re.IGNORECASE):
                suggestion_response_needed = True
                extracted_entities_list = self.transformer.extract_multiple_entities(message)
            else:
                extracted_relation, extracted_entity = self.transformer.extract_text_entities(message)
                print("extracted entity " + extracted_entity)
                print("extracted relation " + extracted_relation)
                message_parts = message.split(":")
                if len(message_parts) > 1 and len(message_parts[0].split("Please answer this question")) > 1:
                    text_query = message_parts[1]
                    factual_answer_needed = message.split(":")[0].split("Please answer this question")[
                                                1] == " with a factual approach"
                    embedding_answer_needed = message.split(":")[0].split("Please answer this question")[
                                                  1] == " with an embedding approach"
                else:
                    factual_answer_needed = True
                    embedding_answer_needed = True
                    text_query = message

        except Exception as e:
            print(e)
            room.post_messages("Sorry, I could not understand your question. Please try rephrasing it.")
            return


        if factual_answer_needed:
            factual_response = self.message_handler.handle_factual_question(text_query, extracted_entity, extracted_relation)
            print("factual response: " + factual_response)
            room.post_messages(factual_response)
        if embedding_answer_needed:
            embedding_response = self.message_handler.handle_embedding_question(text_query, extracted_entity, extracted_relation)
            print("embedding response: " + embedding_response)
            room.post_messages(embedding_response)
        if suggestion_response_needed:
            suggestion_response = self.message_handler.handle_suggestion_question(message, extracted_entities_list)
            print("suggestion response: " + suggestion_response)
            room.post_messages(suggestion_response)

    def on_new_reaction(self, reaction : str, message_ordinal : int, room : Chatroom):
        print(reaction)
        """Callback function to handle new reactions."""
        pass


if __name__ == '__main__':
    agent = Agent(USERNAME, PASSWORD)
    print('Agent is listening for messages...')
    agent.listen()