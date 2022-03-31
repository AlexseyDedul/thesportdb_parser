import asyncpg
import logging


logger = logging.getLogger(__name__)


async def insert_countries(pool: asyncpg.pool.Pool, countries: dict):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            for country in countries['countries']:
                countryExist = await conn.fetchrow(
                    'SELECT * FROM countries WHERE name_en=$1', country['name_en'])
                if countryExist is None:
                    await conn.execute('''
                            INSERT INTO countries(name_en) VALUES($1)
                        ''', country['name_en'])
                else:
                    await update_countries(pool, country)
        except Exception as e:
            await tr.rollback()
            logger.error(f"Transaction rollback. Countries don`t be insert. Exception: {e}")
        else:
            await tr.commit()


async def update_countries(pool: asyncpg.pool.Pool, country: dict):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            await conn.execute('''
                                UPDATE countries
                                SET name_en=$1
                                WHERE name_en=$1
                                ''', country['name_en'])
        except Exception as e:
            await tr.rollback()
            logger.error(f"Transaction rollback. Countries don`t be update. Exception: {e}")
        else:
            await tr.commit()

