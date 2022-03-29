import asyncpg

from app.thesportsdb_parser.events import get_event_ids_db
from thesportsdb.events import eventTVByEvent


async def get_events_tv_api(events: list) -> list:
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


async def update_channel(pool: asyncpg.pool.Pool, event: dict):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            await conn.execute('''
                                UPDATE channel
                                SET strChannel=$1,
                                WHERE idChannel=$2
                                ''',
                               event['strChannel'],
                               int(event['idChannel']))
            print('updated channel')
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()


async def insert_channel(pool: asyncpg.pool.Pool, events: list):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            for event in events:
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
                else:
                    await update_channel(pool, event)
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()


async def update_events_tv(pool: asyncpg.pool.Pool, event: dict):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            await conn.execute('''
                                UPDATE eventTV
                                SET idEvent=$1,
                                idChannel=$2,
                                strCountry=$3,
                                strLogo=$4,
                                strSeason=$5,
                                strTime=$6,
                                dateEvent=$7,
                                strTimeStamp=$8
                                WHERE id=$9
                                ''',
                               int(event['idEvent']),
                               int(event['idChannel']),
                               event['strCountry'],
                               event['strLogo'],
                               event['strSeason'],
                               event['strTime'],
                               event['dateEvent'],
                               event['strTimeStamp'],
                               int(event['id']))
            print('update event tv')
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()


async def insert_events_tv(pool: asyncpg.pool.Pool, events_ids: list = None):
    if events_ids is None:
        events_ids = await get_event_ids_db(pool)
    events = await get_events_tv_api(events_ids)
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
                else:
                    await update_events_tv(pool, event)
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()
