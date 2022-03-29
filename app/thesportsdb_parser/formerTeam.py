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


async def update_former_team(pool: asyncpg.pool.Pool, former: dict):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            await conn.execute('''
                                UPDATE formerTeam
                                SET idPlayer=$1,
                                idTeam=$2,
                                strSport=$3,
                                strPlayer=$4,
                                strFormerTeam=$5,
                                strMoveType=$6,
                                strTeamBadge=$7,
                                strJoined=$8,
                                strDeparted=$9
                                WHERE idFormerTeam=$10
                                ''',
                               int(former['idPlayer']),
                               int(former['idFormerTeam']),
                               former['strSport'],
                               former['strPlayer'],
                               former['strFormerTeam'],
                               former['strMoveType'],
                               former['strTeamBadge'],
                               former['strJoined'],
                               former['strDeparted'],
                               int(former['id']))
            print('updated former team')
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()


async def insert_former_teams(pool: asyncpg.pool.Pool, players: list):
    former_teams = await get_former_team_api(pool, players)
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
                else:
                    await update_former_team(pool, former)
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()
