import asyncpg

from app.thesportsdb_parser.events import get_event_ids_db
from thesportsdb.events import eventTimeline

import logging


logger = logging.getLogger(__name__)


async def get_timeline_api(events: list) -> list:
    list_timelines = []
    if events:
        for event in events:
            try:
                timelines = await eventTimeline(str(event['idevent']))
                for timeline in timelines['timeline']:
                    list_timelines.append(timeline)
            except:
                logger.warning(f"Timeline not found by id event: {event['idevent']}")
                continue
    return list_timelines


async def update_timeline(pool: asyncpg.pool.Pool, timeline: dict):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            await conn.execute('''
                                UPDATE timeline
                                SET idEvent=$1,
                                idPlayer=$2,
                                idTeam=$3,
                                strTimeline=$4,
                                strTimelineDetail=$5,
                                strHome=$6,
                                strEvent=$7,
                                strCountry=$8,
                                idAssist=$9,
                                strAssist=$10,
                                intTime=$11,
                                strComment=$12,
                                dateEvent=$13,
                                strSeason=$14
                                WHERE idTimeline=$15
                                ''',
                               int(timeline['idEvent']),
                               int(timeline['idPlayer']),
                               int(timeline['idTeam']),
                               timeline['strTimeline'],
                               timeline['strTimelineDetail'],
                               timeline['strHome'],
                               timeline['strEvent'],
                               timeline['strCountry'],
                               int(timeline['idAssist']) if timeline['idAssist'] else 0,
                               timeline['strAssist'],
                               timeline['intTime'],
                               timeline['strComment'],
                               timeline['dateEvent'],
                               timeline['strSeason'],
                               int(timeline['idTimeline']))
        except Exception as e:
            await tr.rollback()
            logger.error(f"Transaction rollback. Timeline don`t be update. Exception: {e}")
        else:
            await tr.commit()


async def insert_timeline(pool: asyncpg.pool.Pool, event_ids: list = None):
    if event_ids is None:
        event_ids = await get_event_ids_db(pool)
    timelines = await get_timeline_api(event_ids)
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            for timeline in timelines:
                timeline_exist = await conn.fetchrow('''
                                                SELECT idTimeline
                                                FROM timeline
                                                WHERE idTimeline=$1
                                                ''', int(timeline['idTimeline']))
                if timeline_exist is None:
                    await conn.execute('''
                                        INSERT INTO timeline(
                                        idTimeline,
                                        idEvent,
                                        idPlayer,
                                        idTeam,
                                        strTimeline,
                                        strTimelineDetail,
                                        strHome,
                                        strEvent,
                                        strCountry,
                                        idAssist,
                                        strAssist,
                                        intTime,
                                        strComment,
                                        dateEvent,
                                        strSeason)
                                        VALUES(
                                        $1, $2, $3,
                                        $4, $5, $6,
                                        $7, $8, $9, 
                                        $10, $11, $12, 
                                        $13, $14, $15
                                        )
                                        ''', int(timeline['idTimeline']),
                                       int(timeline['idEvent']),
                                       int(timeline['idPlayer']),
                                       int(timeline['idTeam']),
                                       timeline['strTimeline'],
                                       timeline['strTimelineDetail'],
                                       timeline['strHome'],
                                       timeline['strEvent'],
                                       timeline['strCountry'],
                                       int(timeline['idAssist']) if timeline['idAssist'] else 0,
                                       timeline['strAssist'],
                                       timeline['intTime'],
                                       timeline['strComment'],
                                       timeline['dateEvent'],
                                       timeline['strSeason'])
                else:
                    await update_timeline(pool, timeline)
        except Exception as e:
            await tr.rollback()
            logger.error(f"Transaction rollback. Timeline don`t be insert. Exception: {e}")
        else:
            await tr.commit()
