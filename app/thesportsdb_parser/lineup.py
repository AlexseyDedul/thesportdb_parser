import asyncpg

from app.thesportsdb_parser.events import get_event_ids_db
from thesportsdb.events import eventLineup


async def get_lineup_api(events: list) -> list:
    list_lineups = []
    if events:
        for event in events:
            try:
                lineups = await eventLineup(str(event['idevent']))
                for lineup in lineups['lineup']:
                    list_lineups.append(lineup)
            except:
                continue
    return list_lineups


async def update_lineups(pool: asyncpg.pool.Pool, lineup: dict):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            await conn.execute('''
                                UPDATE lineup
                                SET idEvent=$1,
                                idPlayer=$2,
                                idTeam=$3,
                                strEvent=$4,
                                strPosition=$5,
                                strPositionShort=$6,
                                strFormation=$7,
                                strHome=$8,
                                strSubstitute=$9,
                                intSquadNumber=$10,
                                strCountry=$11,
                                strSeason=$12
                                WHERE idLineup=$13
                                ''',
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
                               lineup['strSeason'],
                               int(lineup['idLineup']))
            print("updated lineup")
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()


async def insert_lineups(pool: asyncpg.pool.Pool, events_ids: list = None):
    if events_ids is None:
        events_ids = await get_event_ids_db(pool)
    lineups = await get_lineup_api(events_ids)
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
                else:
                    await update_lineups(pool, lineup)
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()
