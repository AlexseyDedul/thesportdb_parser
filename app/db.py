import asyncpg


class Database():
    def __init__(self, user: str, password: str, database: str, host: str):
        if user is not None:
            self.user = user
        else:
            print("User none")

        if password is not None:
            self.password = password
        else:
            print("Pass none")

        if database is not None:
            self.database = database
        else:
            print("DB none")

        if host is not None:
            self.host = host
        else:
            print("Host none")

        self.pool = None

    async def create_tables(self):
        async with self.pool.acquire() as conn:
            tr = conn.transaction()
            await tr.start()
            try:
                await conn.execute('''
                                    CREATE TABLE IF NOT EXISTS sports(
                                        idSport integer PRIMARY KEY,
                                        strSport text,
                                        strFormat text
                                    );
                                    CREATE TABLE IF NOT EXISTS countries(
                                        id serial PRIMARY KEY,
                                        name_en text
                                    );
                                    CREATE TABLE IF NOT EXISTS league(
                                        idLeague integer PRIMARY KEY,
                                        strSport text,
                                        strLeague text,
                                        strLeagueAlternate text,
                                        intDivision integer,
                                        strCurrentSeason text,
                                        intFormedYear integer,
                                        dateFirstEvent text,
                                        strCountry text,
                                        strWebsite text,
                                        strFacebook text,
                                        strTwitter text,
                                        strYoutube text,
                                        strRSS text,
                                        strDescriptionEN text,
                                        strDescriptionRU text,
                                        strTvRights text,
                                        strFanart1 text,
                                        strFanart2 text,
                                        strFanart3 text,
                                        strFanart4 text,
                                        strBanner text,
                                        strBadge text,
                                        strLogo text,
                                        strPoster text,
                                        strTrophy text,
                                        strNaming text,
                                        strComplete text
                                    );
                                    CREATE TABLE IF NOT EXISTS team(
                                        idTeam integer PRIMARY KEY,
                                        strTeam text,
                                        strAlternate text,
                                        intFormedYear integer,
                                        strSport text,
                                        strLeague text,
                                        strDivision text,
                                        strManager text,
                                        strStadium text,
                                        strRSS text,
                                        strStadiumThumb text,
                                        strStadiumDescription text,
                                        strStadiumLocation text,
                                        intStadiumCapacity text,
                                        strWebsite text,
                                        strFacebook text,
                                        strTwitter text,
                                        strYoutube text,
                                        strInstagram text,
                                        strDescriptionEN text,
                                        strDescriptionRU text,
                                        strGender text,
                                        strCountry text,
                                        strTeamBadge text,
                                        strTeamJersey text,
                                        strTeamLogo text,
                                        strTeamBanner text,
                                        strTeamFanart1 text,
                                        strTeamFanart2 text,
                                        strTeamFanart3 text,
                                        strTeamFanart4 text
                                    );
                                    CREATE TABLE IF NOT EXISTS teamLeague(
                                        idLeague integer REFERENCES league (idLeague),
                                        idTeam integer REFERENCES team (idTeam)
                                    );                                    
                                    CREATE TABLE IF NOT EXISTS events(
                                        idEvent integer PRIMARY KEY,
                                        idLeague integer REFERENCES league (idLeague),
                                        strSport text,
                                        strEvent text,
                                        strEventAlternate text,
                                        idHomeTeam integer REFERENCES team (idTeam),
                                        idAwayTeam integer REFERENCES team (idTeam),
                                        strFilename text,
                                        strSeason text,
                                        strDescriptionEN text,
                                        intHomeScore integer,
                                        intRound integer,
                                        intAwayScore integer,
                                        intSpectators integer,
                                        strOfficial text,
                                        strTimestamp text,
                                        dateEvent text,
                                        dateEventLocal text,
                                        strTime text,
                                        strTimeLocal text,
                                        strTVStation text,
                                        strResult text,
                                        strVenue text,
                                        strCountry text,
                                        strCity text,
                                        strPoster text,
                                        strSquare text,
                                        strFanart text,
                                        strThumb text,
                                        strBanner text,
                                        strMap text,
                                        strVideo text,
                                        strStatus text,
                                        strPostponed text,
                                        strLocked text
                                    );
                                    CREATE TABLE IF NOT EXISTS tables(
                                        idStanding integer PRIMARY KEY,
                                        intRank integer,
                                        idTeam integer REFERENCES team (idTeam),
                                        idLeague integer REFERENCES league (idLeague),
                                        strSeason text,
                                        strForm text,
                                        strDescription text,
                                        intPlayed integer,
                                        intWin integer,
                                        intLoss integer,
                                        intDraw integer,
                                        intGoalsFor integer,
                                        intGoalsAgainst integer,
                                        intGoalDifference integer,
                                        intPoints integer,
                                        dateUpdated text
                                    );
                                    CREATE TABLE IF NOT EXISTS player(
                                        idPlayer integer PRIMARY KEY,
                                        idPlayerManager integer REFERENCES playerManager (idManager),
                                        strNationality text,
                                        strPlayer text,
                                        strSeason text,
                                        strTeam text,
                                        strSport text,
                                        dateBorn text,
                                        strNumber text,
                                        dateSigned text,
                                        strSigning text,
                                        strWage text,
                                        strOutfitter text,
                                        strKit text,
                                        strAgent text,
                                        strBirthLocation text,
                                        strDescriptionEN text,
                                        strDescriptionRU text,
                                        strGender text,
                                        strSide text,
                                        strPosition text,
                                        strFacebook text,
                                        strWebsite text,
                                        strTwitter text,
                                        strInstagram text,
                                        strYoutube text,
                                        strHeight text,
                                        strWeight text,
                                        strThumb text,
                                        strFanart1 text,
                                        strFanart2 text,
                                        strFanart3 text,
                                        strFanart4 text
                                    );
                                    CREATE TABLE IF NOT EXISTS playerTeam(
                                        idPlayer integer REFERENCES player (idPlayer),
                                        idTeam integer REFERENCES team (idTeam)
                                    ); 
                                    CREATE TABLE IF NOT EXISTS playerManager(
                                        idManager integer PRIMARY KEY,
                                        strManager text
                                    ); 
                                ''')
            except:
                await tr.rollback()
                raise
            else:
                await tr.commit()
            print("create")

    async def get_pool_connection(self) -> asyncpg.pool.Pool:
        try:
            # print(self.user,self.password,self.database,self.host)
            self.pool = await asyncpg.create_pool(user=self.user,
                                             password=self.password,
                                             database=self.database,
                                             host=self.host)

            # await self.drop_tables()
            # await self.create_tables()
            return self.pool
        except:
            print("Pool connection doesn`t created.")

    async def drop_tables(self):
        async with self.pool.acquire() as conn:
            tr = conn.transaction()
            await tr.start()
            try:
                await conn.execute('''
                                    DROP TABLE IF EXISTS events;
                                    DROP TABLE IF EXISTS tables;
                                    DROP TABLE IF EXISTS playerTeam;
                                    DROP TABLE IF EXISTS player CASCADE;
                                    ''')
            except:
                await tr.rollback()
                raise
            else:
                await tr.commit()
        print("drop")

    async def delete_pool_connection(self, pool):
        await self.drop_tables()
        await pool.close()
        if (await self.pool == None):
            print("Pool close")
