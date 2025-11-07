from speakeasypy import Chatroom, EventType, Speakeasy

from cred import USERNAME, PASSWORD
from src.message_handler import MessageHandler
import re

DEFAULT_HOST_URL = 'https://speakeasy.ifi.uzh.ch'


class Agent:
    def __init__(self, username: str, password: str):
        self.username = username
        # Initialize the Speakeasy Python framework and login.
        self.speakeasy = Speakeasy(host=DEFAULT_HOST_URL, username=username, password=password)
        self.speakeasy.login()  # This framework will help you log out automatically when the program terminates.

        self.speakeasy.register_callback(self.on_new_message, EventType.MESSAGE)
        self.speakeasy.register_callback(self.on_new_reaction, EventType.REACTION)

        self.message_handler = MessageHandler()

    def listen(self):
        """Start listening for events."""
        self.speakeasy.start_listening()

    def on_new_message(self, message : str, room : Chatroom):
        """Callback function to handle new messages."""
        # Implement your agent logic here, e.g., respond to the message.
        room.post_messages("Let's have a look...")
        message_parts = message.split(":")
        if len(message_parts) > 1:
            text_query = message.split(":")[1]
            both_answers_needed = message.split(":")[0].split("Please answer this question")[1] == ""
        elif re.search(r"\brecommend\b|\bsimilar\b|\blike\b|\bsuggestions?\b", message, re.IGNORECASE):
            both_answers_needed = False
            text_query = message
        else:
            both_answers_needed = True
            text_query = message
        factual_answer_needed = both_answers_needed or message.split(":")[0].split("Please answer this question")[1] == " with a factual approach"
        embedding_answer_needed = both_answers_needed or message.split(":")[0].split("Please answer this question")[1] == " with an embedding approach"
        factual_response, embedding_response, suggestion_response = self.message_handler.handle_message(text_query)
        if factual_response == "" and factual_answer_needed:
            factual_response = 'I could not find a factual answer to your question. Please try rephrasing it or ask something else.'
        if embedding_response == "" and embedding_answer_needed:
            embedding_response = 'I could not find an answer based on embeddings to your question. Please try rephrasing it or ask something else.'
        if factual_answer_needed:
            print("factual response: " + factual_response)
            room.post_messages(factual_response)
        if embedding_answer_needed:
            print("embedding response: " + embedding_response)
            room.post_messages(embedding_response)
        else:
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