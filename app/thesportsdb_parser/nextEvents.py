import asyncpg

from app.thesportsdb_parser.eventTV import insert_events_tv
from app.thesportsdb_parser.events import insert_events, insert_event_stats
from app.thesportsdb_parser.leagues import get_leagues_ids_list
from app.thesportsdb_parser.lineup import insert_lineups
from app.thesportsdb_parser.timeline import insert_timeline
from thesportsdb.events import nextLeagueEvents


async def get_next_events_api(pool: asyncpg.pool.Pool) -> list:
    leagues = await get_leagues_ids_list(pool)
    events_list = []
    for league in leagues[:10]:
        try:
            events = await nextLeagueEvents(str(league['idleague']))
            for event in events['events']:
                events_list.append(event)
        except:
            continue
    return events_list


async def work_with_events(pool: asyncpg.pool.Pool):
    events = await get_next_events_api(pool)
    await insert_events(pool, events)
    await insert_event_stats(pool, events)
    await insert_events_tv(pool, events)
    await insert_lineups(pool, events)
    await insert_timeline(pool, events)
