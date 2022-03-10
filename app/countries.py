import asyncpg


async def insert_countries(pool: asyncpg.pool.Pool, countries: dict):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            if not (await _count_countries_from_db(pool, countries)):
                for i in countries['countries']:
                    countryExist = await conn.fetch(
                        'SELECT * FROM countries WHERE name_en=$1', i['name_en'])
                    if (countryExist == []):
                        await conn.execute('''
                                INSERT INTO countries(name_en) VALUES($1)
                            ''', i['name_en'])
            else:
                print("doesn`t insert countries.")
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()
    print("insert_countries")


async def _count_countries_from_db(pool: asyncpg.pool.Pool, countries: dict) -> bool:
    async with pool.acquire() as conn:
        countries_from_db = await conn.fetch('''
            SELECT count(*) FROM countries
        ''')

        if countries_from_db[0]['count'] != len(countries['countries']):
            return False
        return True
