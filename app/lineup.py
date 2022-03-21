import asyncpg

from app.events import get_event_ids_db
from thesportsdb.events import eventLineup


async def get_lineup_api(pool: asyncpg.pool.Pool) -> list:
    events = await get_event_ids_db(pool)
    list_lineups = []
    if events:
        for event in events:
            try:
                lineups = await eventLineup(str(event['idevent']))
                # lineups = await eventLineup('1032723')
                for lineup in lineups['lineup']:
                    list_lineups.append(lineup)
            except:
                continue
    return list_lineups


async def get_lineups_count_db(pool: asyncpg.pool.Pool) -> int:
    async with pool.acquire() as conn:
        count = await conn.fetchrow('''
                                    SELECT count(*)
                                    FROM lineup
                                    ''')
        return count['count']


async def insert_lineups(pool: asyncpg.pool.Pool):
    lineups = await get_lineup_api(pool)
    if await get_lineups_count_db(pool) != len(lineups):
        async with pool.acquire() as conn:
            tr = conn.transaction()
            await tr.start()
            try:
                for lineup in lineups:
                    lineup_exist = await conn.fetchrow('''
                                                    SELECT idLineup
                                                    FROM lineup
                                                    WHERE idLineup=$1
                                                    ''', int(lineup['idLineup']))
                    if lineup_exist is None:
                        await conn.execute('''
                                            INSERT INTO lineup(
                                            idLineup,
                                            idEvent,
                                            idPlayer,
                                            idTeam,
                                            strEvent,
                                            strPosition,
                                            strPositionShort,
                                            strFormation,
                                            strHome,
                                            strSubstitute,
                                            intSquadNumber,
                                            strCountry,
                                            strSeason)
                                            VALUES(
                                            $1, $2, $3,
                                            $4, $5, $6,
                                            $7, $8, $9, 
                                            $10, $11, $12, 
                                            $13
                                            )
                                            ''', int(lineup['idLineup']),
                                            int(lineup['idEvent']),
                                            int(lineup['idPlayer']),
                                            int(lineup['idTeam']),
                                            lineup['strEvent'],
                                            lineup['strPosition'],
                                            lineup['strPositionShort'],
                                            lineup['strFormation'],
                                            lineup['strHome'],
                                            lineup['strSubstitute'],
                                            lineup['intSquadNumber'],
                                            lineup['strCountry'],
                                            lineup['strSeason'])
                        print("insert lineup")
            except:
                await tr.rollback()
                raise
            else:
                await tr.commit()
