import os

from dotenv import load_dotenv

load_dotenv()

DB_SETTINGS = dict(
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    database_name=os.getenv("POSTGRES_DB"),
    host=os.getenv("POSTGRES_HOST"),
    port="5432",
)
