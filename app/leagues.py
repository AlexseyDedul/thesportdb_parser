import asyncpg


async def insert_leagues(pool: asyncpg.pool.Pool, leagues: dict):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            if not (await _count_leagues_from_db(pool, leagues)):
                for i in leagues['leagues']:
                    leagueExist = await conn.fetch(
                        'SELECT idLeague FROM league WHERE idLeague=$1', i['idLeague'])
                    if (leagueExist == []):
                        await conn.execute('''
                                INSERT INTO league(idLeague, strLeague) VALUES($1, $2)
                            ''', i['idLeague'], i['strLeague'])
            else:
                print('doesn`t insert')
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()
    print("insertLeagues end methods")


async def _count_leagues_from_db(pool: asyncpg.pool.Pool, leagues: dict) -> bool:
    async with pool.acquire() as conn:
        leagues_from_db = await conn.fetch('''
            SELECT count(idLeague) FROM league
        ''')

        if leagues_from_db[0]['count'] != len(leagues['leagues']):
            return False
        return True


async def get_leagues_ids_list(pool: asyncpg.pool.Pool) -> list:
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            leagues = await conn.fetch(
                'SELECT idleague FROM league')
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()
        print("getLeaguesById")
        return leagues