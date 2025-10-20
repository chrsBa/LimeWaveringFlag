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
- Make sure Ollama is installed: https://ollama.com/download
## Add you credentials
Create a file .cred.py in the root directory (LimeWaveringFlag) and add your credentials:
```
 USERNAME = "your_username"
 PASSWORD = "your_password"
```

## Data
- Copy graph.nt file into src folder

## Run the application
```
python agent.py
```

