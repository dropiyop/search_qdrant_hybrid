import dotenv
import os
import pathlib


env_path = pathlib.Path(__file__).resolve().parent / '.env'
dotenv.load_dotenv(dotenv_path=env_path)

TOKEN = os.getenv("TOKEN")
OPENAI_TOKEN = os.getenv("OPENAI_TOKEN")
PROXY_SERVER = os.getenv("PROXY_SERVER")
PROXY_PORT = os.getenv("PROXY_PORT")
MODEL = os.getenv("MODEL")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DB = os.getenv("PG_DB")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
QDRANT_HOST= os.getenv("QDRANT_HOST")
QDRANT_PORT = os.getenv("QDRANT_PORT")
QDRANT_KEY = os.getenv("QDRANT_KEY")