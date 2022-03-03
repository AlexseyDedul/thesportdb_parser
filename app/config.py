import os

from app.db import Database


async def create_app():
    db = Database(user=os.environ.get("USER"),
                  password=os.environ.get("PASS"),
                  database=os.environ.get("DB"),
                  host=os.environ.get("HOST"))
    app = {
        'db': db,
        'pool': await db.get_pool_connection()
    }
    return app
