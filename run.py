import asyncio
from time import time
import asyncpg
from config import host, user, password, database
from thesportsdb.countries import allCountries
from thesportsdb.leagues import allLeagues
from thesportsdb.sports import allSports
from thesportsdb.teams import leagueTeams

#####################################################################

# async with asyncpg.create_pool(user=user, password=password, database=database, host=host) as pool:
#     async with pool.acquire() as conn:
#         async with conn.transaction():
#             # Execute a statement to create a new table.
#
#             await conn.execute('''
#                     CREATE TABLE IF NOT EXISTS sports(
#                         id serial PRIMARY KEY,
#                         name text
#                     );
#                     CREATE TABLE IF NOT EXISTS countries(
#                         id serial PRIMARY KEY,
#                         name_en text
#                     );
#                     CREATE TABLE IF NOT EXISTS league(
#                         id serial PRIMARY KEY,
#                         idLeague text,
#                         strLeague text
#                     );
#                 ''')
#
#             # Insert a record into the created table.
#             for i in sports['sports']:
#                 print(i)
#                 await conn.execute('''
#                         INSERT INTO sports(name) VALUES($1)
#                     ''', i['strSport'])
#
#             for i in countries['countries']:
#                 print(i)
#                 await conn.execute('''
#                         INSERT INTO countries(name_en) VALUES($1)
#                     ''', i['name_en'])
#             for i in leagues['leagues']:
#                 print(i)
#                 await conn.execute('''
#                         INSERT INTO league(idLeague, strLeague) VALUES($1, $2)
#                     ''', i['idLeague'], i['strLeague'])
#
#             # Select a row from the table.
#             league = await conn.fetch(
#                 'SELECT * FROM league')
#             # *row* now contains
#             # asyncpg.Record(id=1, name='Bob', dob=datetime.date(1984, 3, 1))
#
#             # for i in league:
#             #     print(await leagueTeams(i['idleague']))
#             # Close the connection.

######################################################################

#
# conn = await asyncpg.connect(user=user, password=password, database=database, host=host)
#     # Execute a statement to create a new table.
#
#     await conn.execute('''
#                 CREATE TABLE IF NOT EXISTS sports(
#                     id serial PRIMARY KEY,
#                     name text
#                 );
#                 CREATE TABLE IF NOT EXISTS countries(
#                     id serial PRIMARY KEY,
#                     name_en text
#                 );
#                 CREATE TABLE IF NOT EXISTS league(
#                     id serial PRIMARY KEY,
#                     idLeague text,
#                     strLeague text
#                 );
#             ''')
#
#     # Insert a record into the created table.
#     for i in sports['sports']:
#         print(i)
#         await conn.execute('''
#                     INSERT INTO sports(name) VALUES($1)
#                 ''', i['strSport'])
#
#     for i in countries['countries']:
#         print(i)
#         await conn.execute('''
#                     INSERT INTO countries(name_en) VALUES($1)
#                 ''', i['name_en'])
#     for i in leagues['leagues']:
#         print(i)
#         await conn.execute('''
#                     INSERT INTO league(idLeague, strLeague) VALUES($1, $2)
#                 ''', i['idLeague'], i['strLeague'])
#
#     # Select a row from the table.
#     league = await conn.fetch(
#         'SELECT * FROM league')
#     # *row* now contains
#     # asyncpg.Record(id=1, name='Bob', dob=datetime.date(1984, 3, 1))
#
#     # for i in league:
#     #     print(await leagueTeams(i['idleague']))
#     # Close the connection.
#     await conn.close()


async def run():

    t0 = time()
    # await asyncio.gather(
    sports = await asyncio.create_task(allSports())
    countries = await asyncio.create_task(allCountries())
    leagues = await asyncio.create_task(allLeagues())

    # print(await asyncio.create_task(allCountries()))
    # print(leagues)
    # print(await asyncio.create_task(allSports()))

    # for i in range(len(res)):
    #     print(res[i])

    # Establish a connection to an existing database named "test"
    # as a "postgres" user.

    async with asyncpg.create_pool(user=user, password=password, database=database, host=host) as pool:
        async with pool.acquire() as conn:
            async with conn.transaction():
                # Execute a statement to create a new table.

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
                            strTeam text
                        );
                    ''')

                # Insert a record into the created table.
                for i in sports['sports']:
                    # print(i)
                    await conn.execute('''
                            INSERT INTO sports(name) VALUES($1)
                        ''', i['strSport'])

                for i in countries['countries']:
                    # print(i)
                    await conn.execute('''
                            INSERT INTO countries(name_en) VALUES($1)
                        ''', i['name_en'])
                for i in leagues['leagues']:
                    # print(i)
                    await conn.execute('''
                            INSERT INTO league(idLeague, strLeague) VALUES($1, $2)
                        ''', i['idLeague'], i['strLeague'])

                # Select a row from the table.
                league = await conn.fetch(
                    'SELECT idleague FROM league')
                # *row* now contains
                # asyncpg.Record(id=1, name='Bob', dob=datetime.date(1984, 3, 1))

                for i in league:
                    teams = await leagueTeams(i['idleague'])
                    try:
                        # print(teams['teams'][0]['idTeam'])
                        for t in teams['teams']:
                            teamExist = await conn.fetch(
                                'SELECT * FROM teams WHERE idTeam=$1', t['idTeam'])
                            if(teamExist == []):
                                await conn.execute('''
                                                INSERT INTO teams(idTeam, strTeam) VALUES($1, $2)
                                            ''', t['idTeam'], t['strTeam'])
                    except:
                        continue

                teams = await conn.fetch(
                    'SELECT * FROM teams ORDER BY idTeam')
                for i in teams:
                    print(i)

                # Close the connection.
    print(time() - t0)


if __name__ == '__main__':
    asyncio.run(run())