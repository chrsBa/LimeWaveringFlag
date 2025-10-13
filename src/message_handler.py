from query_handler import handle_query


def handle_message(message: str) -> str:
    """
    Orchestrate message processing and response generation.

    Args:
        message (str): The incoming message to be processed.

    Returns:
        str: The response after processing the message.
    """

    response = ""
    
    if(message.startswith("PREFIX")):
        response = handle_query(message)
    else:
        response = "Please enter a valid SPARQL Query"
        
    return response
