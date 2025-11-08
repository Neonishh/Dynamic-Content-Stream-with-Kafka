# config.py
import os

# Always use absolute path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "topics.db")

HOST = "0.0.0.0"
PORT = 5000
DEBUG = True
