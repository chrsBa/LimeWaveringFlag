# LimeWaveringFlag
ATAI Chatbot

## Environment Setup
```
# Create new venv
python -m venv venv

# Activate venv
source venv/bin/activate

# Install project dependencies
pip install -r requirements.txt
```

- Install ollama (https://anaconda.org/conda-forge/ollama)
- Make sure your Ollama server is running
- Pull the Ollama model gemma3:4b (https://medium.com/@gabrielrodewald/running-models-with-ollama-step-by-step-60b6f6125807)
- Pull embbedding model snowflake-arctic-embed:335m
```
conda install conda-forge::ollama
ollama serve
ollama pull gemma3:4b
ollama pull snowflake-arctic-embed:335m
```

## Add you credentials
Create a file .cred.py in the root directory (LimeWaveringFlag) and add your credentials:
```
 USERNAME = "your_username"
 PASSWORD = "your_password"
```

## Data
- Create a data folder LimeWaveringFlag/data
- Copy graph.nt file into the LimeWaveringFlag/data folder
- Copy relation_embeds.npy file into the LimeWaveringFlag/data folder
- Copy entity_embeds.npy file into the LimeWaveringFlag/data folder
- Copy relation_ids.del file into the LimeWaveringFlag/data folder
- Copy entity_ids.del file into the LimeWaveringFlag/data folder
- Copy images.json file into the LimeWaveringFlag/data folder
- Run the entity/label mapping and movie mapping script in graph_db.py (main)
- Run the vector store filling script in vector_store.py (main)

## Run the application
```
python agent.py
```

