import asyncio
from time import time
import asyncpg

from app import create_app
from app.countries import insert_countries
from app.events import insert_events
from app.leagues import insert_leagues, get_leagues_ids_list
from app.sports import insert_sports
from app.teams import insert_teams, get_teams_ids_list
from thesportsdb.countries import allCountries
from thesportsdb.events import nextLeagueEvents, leagueSeasonEvents
from thesportsdb.leagues import allLeagues, leagueSeasonTable
from thesportsdb.players import teamPlayers
from thesportsdb.seasons import allSeason
from thesportsdb.sports import allSports


async def insert_tables(pool: asyncpg.pool.Pool, leagues: list):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            for i in leagues:
                try:
                    seasons = await allSeason(i['idleague'])
                    for s in seasons['seasons']:
                        try:
                            table = await leagueSeasonTable(i['idleague'], s['strSeason'])
                            for t in table['table']:
                                await conn.execute('''
                                                INSERT INTO tables(idStanding) VALUES($1)
                                            ''', t['idStanding'])
                        except:
                            continue
                    print("tables insert")
                except:
                    continue

        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()
        print("insert_tables")


async def insert_players(pool: asyncpg.pool.Pool):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        teams = await get_teams_ids_list(pool)
        try:
            for t in teams:
                try:
                    player = await teamPlayers(t['idteam'])
                    for p in player['player']:
                        await conn.execute('''
                                        INSERT INTO players(idPlayer, idTeamNational, idPlayerManager) VALUES($1, $2, $3)
                                    ''', p['idPlayer'], p['idTeamNational'], p['idPlayerManager'])

                    print("players insert")
                except:
                    continue

        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()
        print("insert_players")


async def run():
    app = await create_app()
    pool = await app['db'].get_pool_connection()
    print(pool)
    # async with asyncpg.create_pool(user=os.environ.get("USER"),
    #                                password=os.environ.get("PASS"),
    #                                database=os.environ.get("DB"),
    #                                host=os.environ.get("HOST")) as pool:

    # while True:
    t0 = time()
    await asyncio.create_task(insert_countries(pool, await allCountries())),
    await asyncio.create_task(insert_sports(pool, await allSports())),
    await asyncio.create_task(insert_leagues(pool, await allLeagues()))
    # await asyncio.create_task(insert_seasons(pool, await allSeason()))

    leagues = await get_leagues_ids_list(pool)
    await asyncio.gather(
        asyncio.create_task(insert_teams(pool, leagues)),
        asyncio.create_task(insert_events(pool, leagues)),
        asyncio.create_task(insert_tables(pool, leagues)),
        asyncio.create_task(insert_players(pool))
    )
    print(time() - t0)

    # await app['db'].delete_pool_connection()

if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(run())
    except KeyboardInterrupt:
        print("Close application.")


