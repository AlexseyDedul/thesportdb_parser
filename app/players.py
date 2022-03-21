import asyncpg

from app.teams import get_teams_ids_list
from thesportsdb.players import teamPlayers


async def get_players_api(pool: asyncpg.pool.Pool) -> list:
    list_players = []
    teams = await get_teams_ids_list(pool)
    for t in teams:
        try:
            player = await teamPlayers(str(t['idteam']))
            for p in player['player']:
                list_players.append(p)
        except:
            continue
    return list_players


async def get_list_players_db(pool: asyncpg.pool.Pool) -> list:
    async with pool.acquire() as conn:
        return await conn.fetch('''
                                SELECT idPlayer 
                                FROM player;
                                ''')


async def insert_players(pool: asyncpg.pool.Pool):
    players = await get_players_api(pool)
    players_db = await get_list_players_db(pool)
    if len(players) != len(players_db) and players:
        async with pool.acquire() as conn:
            tr = conn.transaction()
            await tr.start()
            try:
                for p in players:
                    player_exist = await conn.fetchrow('''
                                                        SELECT * FROM player WHERE idPlayer=$1
                                                        ''', int(p['idPlayer']))
                    if player_exist is None:
                        await conn.execute('''
                                    INSERT INTO player(
                                        idPlayer,
                                        strNationality,
                                        strPlayer,
                                        strTeam,
                                        strSport,
                                        dateBorn,
                                        strNumber,
                                        dateSigned,
                                        strSigning,
                                        strWage,
                                        strOutfitter,
                                        strKit,
                                        strAgent,
                                        strBirthLocation,
                                        strDescriptionEN,
                                        strDescriptionRU,
                                        strGender,
                                        strSide,
                                        strPosition,
                                        strFacebook,
                                        strWebsite,
                                        strTwitter,
                                        strInstagram,
                                        strYoutube,
                                        strHeight,
                                        strWeight,
                                        strThumb,
                                        strFanart1,
                                        strFanart2,
                                        strFanart3,
                                        strFanart4) 
                                    VALUES(
                                    $1, $2, $3, $4, $5, $6, $7, $8,
                                        $9, $10, $11, $12, $13, $14, $15, $16,
                                        $17, $18, $19, $20, $21, $22, $23, $24,
                                        $25, $26, $27, $28, $29, $30, $31
                                    )
                                ''', int(p['idPlayer']),
                                       p['strNationality'],
                                       p['strPlayer'],
                                       p['strTeam'],
                                       p['strSport'],
                                       p['dateBorn'],
                                       p['strNumber'],
                                       p['dateSigned'],
                                       p['strSigning'],
                                       p['strWage'],
                                       p['strOutfitter'],
                                       p['strKit'],
                                       p['strAgent'],
                                       p['strBirthLocation'],
                                       p['strDescriptionEN'],
                                       p['strDescriptionRU'],
                                       p['strGender'],
                                       p['strSide'],
                                       p['strPosition'],
                                       p['strFacebook'],
                                       p['strWebsite'],
                                       p['strTwitter'],
                                       p['strInstagram'],
                                       p['strYoutube'],
                                       p['strHeight'],
                                       p['strWeight'],
                                       p['strThumb'],
                                       p['strFanart1'],
                                       p['strFanart2'],
                                       p['strFanart3'],
                                       p['strFanart4'])
                        print("players insert")
            except:
                await tr.rollback()
                raise
            else:
                await tr.commit()
            print("insert_players")
        await check_teams_in_player(pool, players)


async def check_teams_in_player(pool: asyncpg.pool.Pool, player_team: list):
    for player in player_team:
        await insert_teams_player(pool, int(player['idPlayer']),
                                  int(player['idTeam']))
        await insert_teams_player(pool, int(player['idPlayer']),
                                  int(player['idTeam2'] if player['idTeam2'] else 0))
        if player['idTeamNational'] is not None:
            await insert_teams_player(pool, int(player['idPlayer']), int(player['idTeamNational']))


async def insert_teams_player(pool: asyncpg.pool.Pool, player: int, team: int):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            if team != 0:
                player_team_link_exist = await conn.fetchrow('''
                                                            SELECT * 
                                                            FROM playerTeam
                                                            WHERE idPlayer=$1 AND idTeam=$2; 
                                                            ''', player, team)
                if player_team_link_exist is None:
                    await conn.execute('''
                                        INSERT INTO playerTeam(idPlayer, idTeam) 
                                        VALUES($1, $2)
                                    ''', player, team)
                    print('insert PT')
        except:
            print('rollback PT')
            await tr.rollback()
            raise
        else:
            await tr.commit()
