import asyncio

import asyncpg

from app.thesportsdb_parser.contracts import insert_contracts
from app.thesportsdb_parser.countries import insert_countries
from app.thesportsdb_parser.eventTV import insert_events_tv
from app.thesportsdb_parser.events import insert_event_stats, insert_events
from app.thesportsdb_parser.formerTeam import insert_former_teams
from app.thesportsdb_parser.honourTeam import insert_honours_teams
from app.thesportsdb_parser.lastEvents import work_with_last_events
from app.thesportsdb_parser.leagues import insert_leagues, get_leagues_ids_list
from app.thesportsdb_parser.lineup import insert_lineups
from app.thesportsdb_parser.nextEvents import work_with_events
from app.thesportsdb_parser.players import get_list_players_db, insert_players, check_player_in_db
from app.thesportsdb_parser.sports import insert_sports
from app.thesportsdb_parser.tables import insert_tables
from app.thesportsdb_parser.teams import insert_teams
from app.thesportsdb_parser.timeline import insert_timeline
from thesportsdb.countries import allCountries
from thesportsdb.leagues import allLeagues
from thesportsdb.sports import allSports


async def start_parser(pool: asyncpg.pool.Pool):
    # await insert_countries(pool, await allCountries()),
    # await insert_sports(pool, await allSports()),
    # await insert_leagues(pool, await allLeagues())
    # leagues = await get_leagues_ids_list(pool)
    # await insert_teams(pool, leagues),
    # await insert_events(pool),
    # await insert_players(pool)
    # players = await get_list_players_db(pool)
    await asyncio.gather(
        # asyncio.create_task(insert_tables(pool, leagues)),
        # asyncio.create_task(insert_contracts(pool, players)),
        # asyncio.create_task(insert_former_teams(pool, players)),
        # asyncio.create_task(insert_honours_teams(pool, players)),
        # asyncio.create_task(insert_event_stats(pool)),
        # asyncio.create_task(insert_events_tv(pool)),
        asyncio.create_task(insert_timeline(pool)),
        asyncio.create_task(insert_lineups(pool))
    )


async def tasks_once_a_month(pool: asyncpg.pool.Pool):
    while True:
        await asyncio.sleep(60 * 60 * 24 * 30)
        await insert_countries(pool, await allCountries()),
        await insert_sports(pool, await allSports()),
        await insert_leagues(pool, await allLeagues())
        leagues = await get_leagues_ids_list(pool)
        await insert_teams(pool, leagues),
        await insert_players(pool)
        players = await get_list_players_db(pool)
        await asyncio.gather(
            asyncio.create_task(insert_contracts(pool, players)),
            asyncio.create_task(insert_former_teams(pool, players)),
            asyncio.create_task(insert_honours_teams(pool, players))
        )


async def tasks_once_a_week(pool: asyncpg.pool.Pool):
    while True:
        await asyncio.sleep(60 * 60 * 24 * 7)
        leagues = await get_leagues_ids_list(pool)
        await insert_tables(pool, leagues)


async def tasks_once_a_day(pool: asyncpg.pool.Pool):
    while True:
        await asyncio.sleep(60 * 60 * 12)
        await work_with_events(pool),
        await work_with_last_events(pool)


async def main(app):
    pool = await app['db'].get_pool_connection()
    # await app['db'].drop_tables()
    # await app['db'].create_tables()
    await start_parser(pool)

    # tasks = [
    #     asyncio.create_task(tasks_once_a_month(pool)),
    #     asyncio.create_task(tasks_once_a_week(pool)),
    #     asyncio.create_task(tasks_once_a_day(pool))
    #     ]

    # await asyncio.gather(
    #     *tasks
    # )
