import asyncpg

from app.thesportsdb_parser.events import get_event_ids_db
from thesportsdb.events import eventTVByEvent
import logging


logger = logging.getLogger(__name__)


async def get_events_tv_api(events: list) -> list:
    list_events_tv = []
    if events:
        for event in events:
            try:
                try:
                    id_event = str(event['idEvent'])
                except KeyError:
                    id_event = str(event['idevent'])
                events_tv = await eventTVByEvent(id_event)
                for event_tv in events_tv['tvevent']:
                    list_events_tv.append(event_tv)
            except:
                logger.warning(f"Event TV not found by event id: {id_event}")
                continue
    return list_events_tv


async def update_channel(pool: asyncpg.pool.Pool, event: dict):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            await conn.execute('''
                                UPDATE channel
                                SET strChannel=$1
                                WHERE idChannel=$2
                                ''',
                               event['strChannel'],
                               int(event['idChannel']))
        except Exception as e:
            await tr.rollback()
            logger.error(f"Transaction rollback. Channel don`t be update. Exception: {e}")
        else:
            await tr.commit()


async def insert_channel(pool: asyncpg.pool.Pool, events: list):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            for event in events:
                if event['idChannel'] is not None:
                    channel_exist = await conn.fetchrow('''
                                                        SELECT *
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
                    else:
                        await update_channel(pool, event)
        except Exception as e:
            await tr.rollback()
            logger.error(f"Transaction rollback. Channel don`t be insert. Exception: {e}")
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
                               int(event['idChannel']) if int(event['idChannel']) is not None else 0,
                               event['strCountry'],
                               event['strLogo'],
                               event['strSeason'],
                               event['strTime'],
                               event['dateEvent'],
                               event['strTimeStamp'],
                               int(event['id']))
        except Exception as e:
            await tr.rollback()
            logger.error(f"Transaction rollback. Event TV don`t be update. Exception: {e}")
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
                                       int(event['idChannel']) if int(event['idChannel']) is not None else 0,
                                       event['strCountry'],
                                       event['strLogo'],
                                       event['strSeason'],
                                       event['strTime'],
                                       event['dateEvent'],
                                       event['strTimeStamp'])
                else:
                    await update_events_tv(pool, event)
        except Exception as e:
            await tr.rollback()
            logger.error(f"Transaction rollback. Event TV don`t be insert. Exception: {e}")
        else:
            await tr.commit()
