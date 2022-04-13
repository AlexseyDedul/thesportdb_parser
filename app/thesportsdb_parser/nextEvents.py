import asyncio

import asyncpg

from app.thesportsdb_parser.eventTV import insert_events_tv
from app.thesportsdb_parser.events import insert_events, insert_event_stats, is_teams_exist
from app.thesportsdb_parser.leagues import get_leagues_ids_list
from app.thesportsdb_parser.lineup import insert_lineups
from app.thesportsdb_parser.timeline import insert_timeline
from thesportsdb.events import nextLeagueEvents
import logging


logger = logging.getLogger(__name__)


async def get_next_events_api(pool: asyncpg.pool.Pool) -> list:
    leagues = await get_leagues_ids_list(pool)
    events_list = []
    for league in leagues:
        try:
            events = await nextLeagueEvents(str(league['idleague']))
            if events is not None:
                for event in events['events']:
                    if await is_teams_exist(pool, event):
                        events_list.append(event)
        except:
            logger.warning(f"Next 15 event not found by league id: {league['idleague']}")
            continue
    return events_list


async def work_with_events(pool: asyncpg.pool.Pool):
    events = await get_next_events_api(pool)
    await insert_events(pool, events)
    await insert_event_stats(pool, events)
    await insert_events_tv(pool, events)
    await insert_lineups(pool, events)
    await insert_timeline(pool, events)
