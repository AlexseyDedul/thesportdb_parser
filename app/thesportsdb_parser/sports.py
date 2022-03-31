import asyncio

import asyncpg
import logging


logger = logging.getLogger(__name__)


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
        except Exception as e:
            await tr.rollback()
            logger.error(f"Transaction rollback. Sport don`t be update. Exception: {e}")
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
        except Exception as e:
            await tr.rollback()
            logger.error(f"Transaction rollback. Sport don`t be insert. Exception: {e}")
        else:
            await tr.commit()
    await asyncio.sleep(30)
