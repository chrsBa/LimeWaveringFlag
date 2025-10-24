import logging
import os
from typing import Optional

from lancedb.embeddings import get_registry
from lancedb.pydantic import LanceModel, Vector

logging.getLogger("httpx").setLevel(logging.ERROR)  # or logging.ERROR to suppress more

# https://lancedb.github.io/lancedb/embeddings/default_embedding_functions
# Ollama host is only set if defined in environment variables -> needed for containerization
if os.getenv("OLLAMA_HOST"):
    func = get_registry().get("ollama").create(name="V4lentin1879/jina-bert-code-f16:latest",
                                               host=os.getenv("OLLAMA_HOST"))
else:
    func = get_registry().get("ollama").create(name="V4lentin1879/jina-bert-code-f16:latest")

class EntityMetadata(LanceModel):
    entity: str
    label: str

class TableSchema(LanceModel):
    vector: Vector(func.ndims()) = func.VectorField()
    id: str
    text: str = func.SourceField()
    metadata: Optional[EntityMetadata] = None