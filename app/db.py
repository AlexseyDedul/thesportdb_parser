import asyncpg


class Database():
    def __init__(self, user: str, password: str, database: str, host: str):
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.pool = None


    async def get_pool_connection(self):
        try:
            self.pool = await asyncpg.create_pool(user=self.user,
                                             password=self.password,
                                             database=self.database,
                                             host=self.host)
            return self.pool
        except:
            print("Pool connection doesn`t created.")


    async def delete_pool_connection(self):
        await self.pool.close()
        if (await self.pool == None):
            print("Pool close")
