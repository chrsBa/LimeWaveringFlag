from src import query_handler


def handle_message(message: str) -> str:
    """
    Orchestrate message processing and response generation.

    Args:
        message (str): The incoming message to be processed.

    Returns:
        str: The response after processing the message.
    """
    response = query_handler.handle_query(message)
    return response
