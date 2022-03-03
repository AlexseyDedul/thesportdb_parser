import asyncio
from time import time
import asyncpg

from app import create_app
from thesportsdb.countries import allCountries
from thesportsdb.events import nextLeagueEvents
from thesportsdb.leagues import allLeagues
from thesportsdb.sports import allSports
from thesportsdb.teams import leagueTeams


async def createTables(pool: asyncpg.pool.Pool):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            await conn.execute('''
                                CREATE TABLE IF NOT EXISTS sports(
                                    id serial PRIMARY KEY,
                                    name text
                                );
                                CREATE TABLE IF NOT EXISTS countries(
                                    id serial PRIMARY KEY,
                                    name_en text
                                );
                                CREATE TABLE IF NOT EXISTS league(
                                    id serial PRIMARY KEY,
                                    idLeague text,
                                    strLeague text
                                );
                                CREATE TABLE IF NOT EXISTS teams(
                                    id serial PRIMARY KEY,
                                    idTeam text,
                                    strTeam text,
                                    country text
                                );
                                CREATE TABLE IF NOT EXISTS events(
                                    id serial PRIMARY KEY,
                                    idEvent text,
                                    strEvent text
                                );
                            ''')
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()
        print("create")


async def insertSports(pool: asyncpg.pool.Pool, sports: dict):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            for i in sports['sports']:
                sportExist = await conn.fetch(
                    'SELECT * FROM sports WHERE name=$1', i['strSport'])
                if (sportExist == []):
                    # print(i)
                    await conn.execute('''
                            INSERT INTO sports(name) VALUES($1)
                        ''', i['strSport'])
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()
        print("insertSports")


async def insertCountries(pool: asyncpg.pool.Pool, countries: dict):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            for i in countries['countries']:
                countryExist = await conn.fetch(
                    'SELECT * FROM countries WHERE name_en=$1', i['name_en'])
                if (countryExist == []):
                    await conn.execute('''
                            INSERT INTO countries(name_en) VALUES($1)
                        ''', i['name_en'])
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()
    print("insertCountries")


async def insertLeagues(pool: asyncpg.pool.Pool, leagues: dict):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            for i in leagues['leagues']:
                leagueExist = await conn.fetch(
                    'SELECT idLeague FROM league WHERE idLeague=$1', i['idLeague'])
                if (leagueExist == []):
                    await conn.execute('''
                            INSERT INTO league(idLeague, strLeague) VALUES($1, $2)
                        ''', i['idLeague'], i['strLeague'])
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()
    print("insertLeagues")


async def getLeaguesIdList(pool: asyncpg.pool.Pool) -> list:
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            leagues = await conn.fetch(
                'SELECT idleague FROM league')
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()
        print("getLeaguesById")
        return leagues


async def insertTeams(pool: asyncpg.pool.Pool, leagues: list):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            for i in leagues:
                teams = await leagueTeams(i['idleague'])
                try:
                    for t in teams['teams']:
                        teamExist = await conn.fetch(
                            'SELECT * FROM teams WHERE idTeam=$1', t['idTeam'])
                        if(teamExist == []):
                            await conn.execute('''
                                            INSERT INTO teams(idTeam, strTeam, country) VALUES($1, $2, $3)
                                        ''', t['idTeam'], t['strTeam'], t['strCountry'])
                except:
                    continue
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()
        print("insertTeams")


async def insertEvents(pool: asyncpg.pool.Pool, leagues: list):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            for i in leagues:
                events = await nextLeagueEvents(i['idleague'])
                try:
                    for t in events['events']:
                        # eventsExist = await conn.fetch(
                        #     'SELECT * FROM events WHERE idEvent=$1', t['idEvent'])
                        # if(eventsExist == []):
                        await conn.execute('''
                                        INSERT INTO events(idEvent, strEvent) VALUES($1, $2)
                                    ''', t['idEvent'], t['strEvent'])
                except:
                    continue
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()
        print("insertEvents")


async def dropTables(pool: asyncpg.pool.Pool):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            await conn.execute('''
                                DROP TABLE sports;
                                DROP TABLE countries;
                                DROP TABLE league;
                                DROP TABLE teams;
                                ''')
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()
    print("drop")


async def run():
    app = await create_app()
    pool = app['pool']
    # async with asyncpg.create_pool(user=os.environ.get("USER"),
    #                                password=os.environ.get("PASS"),
    #                                database=os.environ.get("DB"),
    #                                host=os.environ.get("HOST")) as pool:
    t0 = time()

    # print(await asyncio.create_task(allCountries()))
    # print(leagues)
    # print(await asyncio.create_task(allSports()))

    # for i in range(len(res)):
    #     print(res[i])

    # Establish a connection to an existing database named "test"
    # as a "postgres" user.
    await asyncio.create_task(dropTables(pool))
    await createTables(pool)

    await asyncio.gather(
        await asyncio.create_task(insertSports(pool, await allSports())),
        await asyncio.create_task(insertCountries(pool, await allCountries())),
        await asyncio.create_task(insertLeagues(pool, await allLeagues())),
        # await asyncio.create_task(insertTeams(pool, await getLeaguesIdList(pool))),
        await asyncio.create_task(insertEvents(pool, await getLeaguesIdList(pool)))
    )


    await asyncio.create_task(dropTables(pool))

        #
        #
        # teams = await conn.fetch(
        #     '''SELECT *
        #     FROM teams
        #     WHERE country='England'
        #     ORDER BY idTeam
        #     ''')
        # for i in teams:
        #     print(i)

        # Close the connection.
    print(time() - t0)
    await app['db'].delete_pool_connection()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())

