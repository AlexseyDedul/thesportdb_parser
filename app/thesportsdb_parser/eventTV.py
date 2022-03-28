import asyncpg

from app.thesportsdb_parser.events import get_event_ids_db
from thesportsdb.events import eventTVByEvent


async def get_events_tv_api(pool: asyncpg.pool.Pool) -> list:
    events = await get_event_ids_db(pool)
    list_events_tv = []
    if events:
        for event in events:
            try:
                events_tv = await eventTVByEvent(str(event['idevent']))
                for event_tv in events_tv['tvevent']:
                    list_events_tv.append(event_tv)
            except:
                continue
    return list_events_tv


async def get_events_tv_db(pool: asyncpg.pool.Pool) -> int:
    async with pool.acquire() as conn:
        count = await conn.fetchrow('''
                                    SELECT COUNT(*)
                                    FROM eventTV
                                    ''')
        return count['count']


async def insert_channel(pool: asyncpg.pool.Pool, events: list):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            for event in events:
                print(event)
                channel_exist = await conn.fetchrow('''
                                                    SELECT idChannel
                                                    FROM channel
                                                    WHERE idChannel=$1
                                                    ''', int(event['idChannel']))
                if channel_exist is None:
                    await conn.execute('''
                                        INSERT INTO channel(
                                            idChannel,
                                            strChannel)
                                            VALUES(
                                            $1, $2
                                            )
                                        ''', int(event['idChannel']),
                                             event['strChannel'])
                    print('insert channel')
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()


async def insert_events_tv(pool: asyncpg.pool.Pool):
    events = await get_events_tv_api(pool)
    if len(events) != await get_events_tv_db(pool):
        await insert_channel(pool, events)
        async with pool.acquire() as conn:
            tr = conn.transaction()
            await tr.start()
            try:
                for event in events:
                    event_exist = await conn.fetchrow('''
                                                        SELECT id
                                                        FROM eventTV
                                                        WHERE id=$1
                                                        ''', int(event['id']))
                    if event_exist is None:
                        await conn.execute('''
                                            INSERT INTO eventTV(
                                            id,
                                            idEvent,
                                            idChannel,
                                            strCountry,
                                            strLogo,
                                            strSeason,
                                            strTime,
                                            dateEvent,
                                            strTimeStamp)
                                            VALUES(
                                            $1, $2, $3,
                                            $4, $5, $6,
                                            $7, $8, $9
                                            )
                                            ''', int(event['id']),
                                            int(event['idEvent']),
                                            int(event['idChannel']),
                                            event['strCountry'],
                                            event['strLogo'],
                                            event['strSeason'],
                                            event['strTime'],
                                            event['dateEvent'],
                                            event['strTimeStamp'])
                        print('insert event tv')
            except:
                await tr.rollback()
                raise
            else:
                await tr.commit()
