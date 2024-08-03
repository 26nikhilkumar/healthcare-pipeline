#!/bin/bash

# Install virtualenv
pip install --user virtualenv

# Create a virtual environment named 'venv'
python -m virtualenv venv

# Activate the virtual environment
source venv/bin/activate

# Install the required packages from the requirements.txt file
pip install -r requirements.txt

echo "Setup complete. Virtual environment 'venv' is created and dependencies are installed."
