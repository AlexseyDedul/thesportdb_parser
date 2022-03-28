import os

from app.db import Database
from dotenv import load_dotenv
import logging

load_dotenv()


def create_app():
    user = os.environ.get('USER')
    password = os.environ.get('PASS')
    database = os.environ.get('DB')
    host = os.environ.get('HOST')
    db = Database(user=user,
                  password=password,
                  database=database,
                  host=host)

    app = {
        'db': db
    }
    return app
