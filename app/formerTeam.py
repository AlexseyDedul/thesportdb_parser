import asyncio

import asyncpg

from thesportsdb.players import playersFormerTeam


async def get_former_team_api(pool: asyncpg.pool.Pool, players: list) -> list:
    list_former = []
    async with pool.acquire() as conn:
        for p in players:
            former_team = await playersFormerTeam(str(p['idplayer']))
            try:
                for former in former_team['formerteams']:
                    team_exist = await conn.fetchrow('''
                                                    SELECT idTeam 
                                                    FROM team
                                                    WHERE idTeam=$1
                                                    ''', int(former['idFormerTeam']))
                    if team_exist is not None:
                        list_former.append(former)
            except:
                continue
    return list_former


async def get_former_team_db(pool: asyncpg.pool.Pool):
    async with pool.acquire() as conn:
        count_former_teams = await conn.fetchrow('''
                                                SELECT count(*)
                                                FROM formerTeam
                                                ''')
    return count_former_teams['count']


async def insert_former_teams(pool: asyncpg.pool.Pool, players: list):
    former_teams = await get_former_team_api(pool, players)
    if await get_former_team_db(pool) != len(former_teams):
        async with pool.acquire() as conn:
            tr = conn.transaction()
            await tr.start()
            try:
                for former in former_teams:
                    former_team_exist = await conn.fetchrow('''
                                            SELECT idFormerTeam
                                            FROM formerTeam
                                            WHERE idFormerTeam=$1
                                            ''', int(former['id']))
                    if former_team_exist is None:
                        await conn.execute('''
                                            INSERT INTO formerTeam(
                                            idFormerTeam,
                                            idPlayer,
                                            idTeam,
                                            strSport,
                                            strPlayer,
                                            strFormerTeam,
                                            strMoveType,
                                            strTeamBadge,
                                            strJoined,
                                            strDeparted)
                                            VALUES(
                                            $1, $2, $3,
                                            $4, $5, $6,
                                            $7, $8, $9,
                                            $10
                                            )
                                            ''', int(former['id']),
                                            int(former['idPlayer']),
                                            int(former['idFormerTeam']),
                                            former['strSport'],
                                            former['strPlayer'],
                                            former['strFormerTeam'],
                                            former['strMoveType'],
                                            former['strTeamBadge'],
                                            former['strJoined'],
                                            former['strDeparted'])
                        print('former insert')
            except:
                await tr.rollback()
                raise
            else:
                await tr.commit()
