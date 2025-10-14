import time

from speakeasypy import Chatroom, EventType, Speakeasy

from cred import USERNAME, PASSWORD
from src.message_handler import MessageHandler

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
        response = self.message_handler.handle_message(message)
        room.post_messages(response)

    def on_new_reaction(self, reaction : str, message_ordinal : int, room : Chatroom):
        """Callback function to handle new reactions."""
        pass


if __name__ == '__main__':
    agent = Agent(USERNAME, PASSWORD)
    agent.listen()