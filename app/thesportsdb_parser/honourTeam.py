import asyncpg

from thesportsdb.players import playersHonours


async def get_honours_team_api(pool: asyncpg.pool.Pool, players: list) -> list:
    list_honours = []
    async with pool.acquire() as conn:
        for p in players:
            honours_team = await playersHonours(str(p['idplayer']))
            try:
                for honour in honours_team['honors']:
                    team_exist = await conn.fetchrow('''
                                                    SELECT idTeam 
                                                    FROM team
                                                    WHERE idTeam=$1
                                                    ''', int(honour['idTeam']))
                    if team_exist is not None:
                        list_honours.append(honour)
            except:
                continue
    return list_honours


async def get_honours_team_db(pool: asyncpg.pool.Pool):
    async with pool.acquire() as conn:
        count_honours_teams = await conn.fetchrow('''
                                                SELECT count(*)
                                                FROM honoursTeam
                                                ''')
    return count_honours_teams['count']


async def insert_honours_teams(pool: asyncpg.pool.Pool, players: list):
    honours_teams = await get_honours_team_api(pool, players)
    if await get_honours_team_db(pool) != len(honours_teams):
        async with pool.acquire() as conn:
            tr = conn.transaction()
            await tr.start()
            try:
                for honours in honours_teams:
                    honour_team_exist = await conn.fetchrow('''
                                            SELECT idHonoursTeam
                                            FROM honoursTeam
                                            WHERE idHonoursTeam=$1
                                            ''', int(honours['id']))
                    if honour_team_exist is None:
                        await conn.execute('''
                                            INSERT INTO honoursTeam(
                                            idHonoursTeam,
                                            idPlayer,
                                            idTeam,
                                            strSport,
                                            strPlayer,
                                            strTeam,
                                            strHonour,
                                            strSeason,
                                            intChecked)
                                            VALUES(
                                            $1, $2, $3,
                                            $4, $5, $6,
                                            $7, $8, $9
                                            )
                                            ''', int(honours['id']),
                                                    int(honours['idPlayer']),
                                                    int(honours['idTeam']),
                                                    honours['strSport'],
                                                    honours['strPlayer'],
                                                    honours['strTeam'],
                                                    honours['strHonour'],
                                                    honours['strSeason'],
                                                    honours['intChecked'])
                        print('honours insert')
            except:
                await tr.rollback()
                raise
            else:
                await tr.commit()
