import asyncpg

from thesportsdb.teams import leagueTeams


async def insert_teams(pool: asyncpg.pool.Pool, leagues: list):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            for i in leagues:
                try:
                    teams = await leagueTeams(i['idleague'])
                    for t in teams['teams']:
                        teamExist = await conn.fetch(
                            'SELECT * FROM teams WHERE idTeam=$1', t['idTeam'])
                        if(teamExist == []):
                            await conn.execute('''
                                            INSERT INTO teams(idTeam, strTeam, country) VALUES($1, $2, $3)
                                        ''', t['idTeam'], t['strTeam'], t['strCountry'])
                except:
                    continue
                print("teams insert")
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()
        print("insert_teams")