import asyncpg

from app.thesportsdb_parser.events import get_event_ids_db
from thesportsdb.events import eventTimeline


async def get_timeline_api(pool: asyncpg.pool.Pool) -> list:
    events = await get_event_ids_db(pool)
    list_timelines = []
    if events:
        for event in events:
            try:
                timelines = await eventTimeline(str(event['idevent']))
                for timeline in timelines['timeline']:
                    list_timelines.append(timeline)
            except:
                continue
    return list_timelines


async def get_timeline_count_db(pool: asyncpg.pool.Pool) -> int:
    async with pool.acquire() as conn:
        count = await conn.fetchrow('''
                                    SELECT count(*)
                                    FROM timeline
                                    ''')
        return count['count']


async def insert_timeline(pool: asyncpg.pool.Pool):
    timelines = await get_timeline_api(pool)
    if await get_timeline_count_db(pool) != len(timelines):
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
                        print("insert timeline")
            except:
                await tr.rollback()
                raise
            else:
                await tr.commit()
