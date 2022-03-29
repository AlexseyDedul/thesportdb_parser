import asyncio
from time import time

from app import create_app
from app.thesportsdb_parser.contracts import insert_contracts
from app.thesportsdb_parser.countries import insert_countries
from app.thesportsdb_parser.eventTV import insert_events_tv
from app.thesportsdb_parser.events import insert_event_stats, insert_events
from app.thesportsdb_parser.formerTeam import insert_former_teams
from app.thesportsdb_parser.honourTeam import insert_honours_teams
from app.thesportsdb_parser.leagues import insert_leagues, get_leagues_ids_list
from app.thesportsdb_parser.lineup import insert_lineups
from app.thesportsdb_parser.nextEvents import get_next_events_api, work_with_events
from app.thesportsdb_parser.players import get_list_players_db, insert_players
from app.thesportsdb_parser.sports import insert_sports
from app.thesportsdb_parser.tables import insert_tables
from app.thesportsdb_parser.teams import insert_teams
from app.thesportsdb_parser.timeline import insert_timeline
from thesportsdb.countries import allCountries
from thesportsdb.leagues import allLeagues
from thesportsdb.sports import allSports


async def run():
    app = create_app()
    pool = await app['db'].get_pool_connection()
    await app['db'].drop_tables()
    await app['db'].create_tables()

    t0 = time()

    await asyncio.create_task(insert_countries(pool, await allCountries())),
    await asyncio.create_task(insert_sports(pool, await allSports())),
    await asyncio.create_task(insert_leagues(pool, await allLeagues()))
    leagues = await get_leagues_ids_list(pool)
    await asyncio.create_task(insert_teams(pool, leagues)),
    await asyncio.create_task(insert_events(pool)),
    await asyncio.create_task(insert_players(pool))
    players = await get_list_players_db(pool)
    await asyncio.gather(
        asyncio.create_task(insert_tables(pool, leagues)),
        asyncio.create_task(insert_contracts(pool, players)),
        asyncio.create_task(insert_former_teams(pool, players)),
        asyncio.create_task(insert_honours_teams(pool, players)),
        asyncio.create_task(insert_event_stats(pool)),
        asyncio.create_task(insert_events_tv(pool)),
        asyncio.create_task(insert_timeline(pool)),
        asyncio.create_task(insert_lineups(pool))
    )

    while True:
        await work_with_events(pool)

    print(time() - t0)

    # await app['db'].delete_pool_connection()

if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(run())
    except KeyboardInterrupt:
        print("Close application.")


