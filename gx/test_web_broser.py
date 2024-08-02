import webbrowser
import os

# Path to your Data Docs index.html file
data_docs_path = os.path.abspath("uncommitted/data_docs/local_site/index.html")

# Open Data Docs in the default web browser
webbrowser.open(f"file://{data_docs_path}")
