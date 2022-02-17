import asyncio
import datetime
from time import time

import asyncpg
from config import host, user, password, database
from thesportsdb.countries import allCountries
from thesportsdb.leagues import allLeagues
from thesportsdb.sports import allSports


async def run():

    while True:
        t0 = time()
        res = await asyncio.gather(
            asyncio.create_task(allCountries()),
            asyncio.create_task(allLeagues()),
            asyncio.create_task(allSports())
        )

        print(time()-t0)

    # for i in range(len(res)):
    #     print(res[i])

    # Establish a connection to an existing database named "test"
    # as a "postgres" user.
    conn = await asyncpg.connect(user=user, password=password, database=database, host=host)
    # Execute a statement to create a new table.
    await conn.execute('''
            CREATE TABLE IF NOT EXISTS users(
                id serial PRIMARY KEY,
                name text,
                dob date
            )
        ''')

    # Insert a record into the created table.
    await conn.execute('''
            INSERT INTO users(name, dob) VALUES($1, $2)
        ''', 'Bob', datetime.date(1984, 3, 1))

    # Select a row from the table.
    row = await conn.fetchrow(
        'SELECT * FROM users WHERE name = $1', 'Bob')
    # *row* now contains
    # asyncpg.Record(id=1, name='Bob', dob=datetime.date(1984, 3, 1))
    print(row)
    # Close the connection.
    await conn.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
    loop.run_forever()