import asyncpg


async def update_sport(pool: asyncpg.pool.Pool, sport: dict):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            await conn.execute('''
                                UPDATE sports 
                                SET strSport=$1, strFormat=$2 
                                WHERE idSport=$3
                            ''', sport['strSport'], sport['strFormat'], int(sport['idSport']))
            print(f"insert sport {int(sport['idSport'])}")
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()


async def insert_sports(pool: asyncpg.pool.Pool, sports: dict):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            for i in sports['sports']:
                sportExist = await conn.fetchrow(
                    'SELECT * FROM sports WHERE idSport=$1', int(i['idSport']))
                if sportExist is None:
                    await conn.execute('''
                            INSERT INTO sports(idSport, strSport, strFormat) VALUES($1, $2, $3)
                        ''', int(i['idSport']), i['strSport'], i['strFormat'])
                else:
                    await update_sport(pool, i)
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()
        print("insertSports")
