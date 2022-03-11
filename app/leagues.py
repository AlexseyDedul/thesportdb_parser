import asyncpg

from thesportsdb.leagues import leagueInfo


async def insert_leagues(pool: asyncpg.pool.Pool, leagues: dict):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            if not (await _count_leagues_from_db(pool, leagues)):
                for i in leagues['leagues']:
                    leagueExist = await conn.fetch(
                        'SELECT idLeague FROM league WHERE idLeague=$1', i['idLeague'])
                    if (leagueExist == []):
                        league = leagueInfo(i['idLeague'])
                        await conn.execute('''
                                INSERT INTO league(idLeague,
                                        strSport,
                                        strLeague,
                                        strLeagueAlternate,
                                        strDivision,
                                        strCurrentSeason,
                                        intFormedYear,
                                        dateFirstEvent,
                                        strCountry,
                                        strWebsite,
                                        strFacebook,
                                        strTwitter,
                                        strYoutube,
                                        strRSS,
                                        strDescriptionEN,
                                        strDescriptionRU,
                                        strTvRights,
                                        strFanart1,
                                        strFanart2,
                                        strFanart3,
                                        strFanart4,
                                        strBanner,
                                        strBadge,
                                        strLogo,
                                        strPoster,
                                        strTrophy,
                                        strNaming,
                                        strComplete) 
                                VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 
                                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, 
                                $25, $26, $27, $28)
                            ''', league['leagues'][0]["idLeague"],
                                        league['leagues'][0]["strSport"],
                                        league['leagues'][0]["strLeague"],
                                        league['leagues'][0]["strLeagueAlternate"],
                                        league['leagues'][0]["strDivision"],
                                        league['leagues'][0]["strCurrentSeason"],
                                        league['leagues'][0]["intFormedYear"],
                                        league['leagues'][0]["dateFirstEvent"],
                                        league['leagues'][0]["strCountry"],
                                        league['leagues'][0]["strWebsite"],
                                        league['leagues'][0]["strFacebook"],
                                        league['leagues'][0]["strTwitter"],
                                        league['leagues'][0]["strYoutube"],
                                        league['leagues'][0]["strRSS"],
                                        league['leagues'][0]["strDescriptionEN"],
                                        league['leagues'][0]["strDescriptionRU"],
                                        league['leagues'][0]["strTvRights"],
                                        league['leagues'][0]["strFanart1"],
                                        league['leagues'][0]["strFanart2"],
                                        league['leagues'][0]["strFanart3"],
                                        league['leagues'][0]["strFanart4"],
                                        league['leagues'][0]["strBanner"],
                                        league['leagues'][0]["strBadge"],
                                        league['leagues'][0]["strLogo"],
                                        league['leagues'][0]["strPoster"],
                                        league['leagues'][0]["strTrophy"],
                                        league['leagues'][0]["strNaming"],
                                        league['leagues'][0]["strComplete"]
                                           )
            else:
                print('doesn`t insert')
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()
    print("insertLeagues end methods")


async def _count_leagues_from_db(pool: asyncpg.pool.Pool, leagues: dict) -> bool:
    async with pool.acquire() as conn:
        leagues_from_db = await conn.fetch('''
            SELECT count(idLeague) FROM league
        ''')

        if leagues_from_db[0]['count'] != len(leagues['leagues']):
            return False
        return True


async def get_leagues_ids_list(pool: asyncpg.pool.Pool) -> list:
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
