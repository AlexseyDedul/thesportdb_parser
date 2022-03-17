import asyncio

import asyncpg

from thesportsdb.teams import leagueTeams


async def get_teams_by_league(leagues: list) -> dict:
    dict_league_teams = {}
    for i in leagues:
        try:
            await asyncio.sleep(1.5)
            teams = await leagueTeams(str(i['idleague']))
            teams_ids_list = []
            for t in teams['teams']:
                teams_ids_list.append(t)
            dict_league_teams[i['idleague']] = teams_ids_list
        except:
            continue
    return dict_league_teams


async def is_compare_size(pool: asyncpg.pool.Pool, league_team: dict) -> bool:
    async with pool.acquire() as conn:
        count_db = await conn.fetchrow('''
                        SELECT count(*) FROM team;
        ''')
        print(count_db['count'])
        count = set()
        for teams in league_team.values():
            for t in teams:
                count.add(int(t['idTeam']))
        print(len(count))
        if len(count) == count_db['count']:
            return True
        return False


async def get_team_by_id(pool: asyncpg.pool.Pool, id_team: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            'SELECT * FROM team WHERE idTeam=$1', id_team)


async def insert_teams(pool: asyncpg.pool.Pool, leagues: list):
    # dict_league_teams = {}
    dict_league_teams = await get_teams_by_league(leagues)
    if not (await is_compare_size(pool, dict_league_teams)):
        async with pool.acquire() as conn:
            tr = conn.transaction()
            await tr.start()
            try:
                for teams in dict_league_teams.values():

                    for t in teams:

                        team_exist_db = await conn.fetchrow(
                            'SELECT * FROM team WHERE idTeam=$1', int(t['idTeam']))

                        if team_exist_db is None:
                            await conn.execute('''INSERT INTO team(idTeam,
                                        strTeam,
                                        strAlternate,
                                        intFormedYear,
                                        strSport,
                                        strLeague,
                                        strDivision,
                                        strManager,
                                        strStadium,
                                        strRSS,
                                        strStadiumThumb,
                                        strStadiumDescription,
                                        strStadiumLocation,
                                        intStadiumCapacity,
                                        strWebsite,
                                        strFacebook,
                                        strTwitter,
                                        strYoutube,
                                        strInstagram,
                                        strDescriptionEN,
                                        strDescriptionRU,
                                        strGender,
                                        strCountry,
                                        strTeamBadge,
                                        strTeamJersey,
                                        strTeamLogo,
                                        strTeamBanner,
                                        strTeamFanart1,
                                        strTeamFanart2,
                                        strTeamFanart3,
                                        strTeamFanart4) 
                                            VALUES($1, $2, $3, $4, $5, 
                                            $6, $7, $8, $9,
                                            $10, $11, $12,$13, 
                                            $14, $15, $16, $17, 
                                            $18, $19, $20, $21,
                                            $22, $23, $24, $25, 
                                            $26, $27, $28, $29, $30, $31)
                                        ''', int(t['idTeam']),
                                               t['strTeam'],
                                               t['strAlternate'],
                                               int(t['intFormedYear']) if t['intFormedYear'] is not None else 0,
                                               t['strSport'],
                                               t['strLeague'],
                                               t['strDivision'],
                                               t['strManager'],
                                               t['strStadium'],
                                               t['strRSS'],
                                               t['strStadiumThumb'],
                                               t['strStadiumDescription'],
                                               t['strStadiumLocation'],
                                               t['intStadiumCapacity'],
                                               t['strWebsite'],
                                               t['strFacebook'],
                                               t['strTwitter'],
                                               t['strYoutube'],
                                               t['strInstagram'],
                                               t['strDescriptionEN'],
                                               t['strDescriptionRU'],
                                               t['strGender'],
                                               t['strCountry'],
                                               t['strTeamBadge'],
                                               t['strTeamJersey'],
                                               t['strTeamLogo'],
                                               t['strTeamBanner'],
                                               t['strTeamFanart1'],
                                               t['strTeamFanart2'],
                                               t['strTeamFanart3'],
                                               t['strTeamFanart4'])
                            print(
                                f"teams insert {int(t['idTeam'])} compare {await get_team_by_id(pool, int(t['idTeam'])) is None}")

            except:
                await tr.rollback()
                print('teams rollback')
                raise
            else:
                await tr.commit()
                print("teams commit")

        print("insert_teams")
    else:
        print("doesn`t insert tables")
    if await is_compare_size(pool, dict_league_teams):
        await insert_league_teams(pool, dict_league_teams)
    else:
        print("teams doesn`t insert")


async def insert_league_teams(pool: asyncpg.pool.Pool, league_team: dict):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            for item in league_team.items():
                for team in item[1]:
                    team_league_link_exist = await conn.fetchrow('''
                                                                SELECT * 
                                                                FROM teamleague
                                                                WHERE idleague=$1 AND idteam=$2; 
                                                                ''', item[0], int(team['idTeam']))
                    if team_league_link_exist is None:
                        await conn.execute('''
                                            INSERT INTO teamLeague(idLeague, idTeam) 
                                            VALUES($1, $2)
                                        ''', item[0], int(team['idTeam']))
                    print('insert LT')
        except:
            print('rollback LT')
            await tr.rollback()
            raise
        else:
            await tr.commit()


async def get_teams_ids_list(pool: asyncpg.pool.Pool) -> list:
    async with pool.acquire() as conn:
        while True:
            teams = await conn.fetch(
                'SELECT idTeam FROM team')
            if teams:
                print(f"get_teams_ids_list {len(teams)}")
                return teams
            else:
                await asyncio.sleep(30)
