import asyncpg


async def insert_sports(pool: asyncpg.pool.Pool, sports: dict):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            if not (await _count_sports_from_db(pool, sports)):
                for i in sports['sports']:
                    sportExist = await conn.fetch(
                        'SELECT * FROM sports WHERE name=$1', i['strSport'])
                    if (sportExist == []):
                        # print(i)
                        await conn.execute('''
                                INSERT INTO sports(name) VALUES($1)
                            ''', i['strSport'])
            else:
                print("doesn`t insert sports.")
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()
        print("insertSports")


async def _count_sports_from_db(pool: asyncpg.pool.Pool, sports: dict) -> bool:
    async with pool.acquire() as conn:
        sports_from_db = await conn.fetch('''
            SELECT count(*) FROM sports
        ''')

        if sports_from_db[0]['count'] != len(sports['sports']):
            return False
        return True