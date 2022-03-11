import asyncpg


class Database():
    def __init__(self, user: str, password: str, database: str, host: str):
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.pool = None

    async def create_tables(self):
        async with self.pool.acquire() as conn:
            tr = conn.transaction()
            await tr.start()
            try:
                await conn.execute('''
                                    CREATE TABLE IF NOT EXISTS sports(
                                        idSport PRIMARY KEY,
                                        strSport text,
                                        strFormat text
                                    );
                                    CREATE TABLE IF NOT EXISTS countries(
                                        id serial PRIMARY KEY,
                                        name_en text
                                    );
                                    CREATE TABLE IF NOT EXISTS league(
                                        idLeague PRIMARY KEY,
                                        strSport text,
                                        strLeague text,
                                        strLeagueAlternate text,
                                        strDivision text,
                                        strCurrentSeason text,
                                        intFormedYear text,
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
                                    CREATE TABLE IF NOT EXISTS tables(
                                        id serial PRIMARY KEY,
                                        idStanding text
                                    );
                                    CREATE TABLE IF NOT EXISTS players(
                                        id serial PRIMARY KEY,
                                        idPlayer text,
                                        idTeamNational text,
                                        idPlayerManager text
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
            await self.create_tables()
            return self.pool
        except:
            print("Pool connection doesn`t created.")

    async def drop_tables(self):
        async with self.pool.acquire() as conn:
            tr = conn.transaction()
            await tr.start()
            try:
                await conn.execute('''
                                    DROP TABLE IF EXISTS sports;
                                    DROP TABLE IF EXISTS countries;
                                    DROP TABLE IF EXISTS league;
                                    DROP TABLE IF EXISTS teams;
                                    DROP TABLE IF EXISTS events;
                                    DROP TABLE IF EXISTS tables;
                                    DROP TABLE IF EXISTS players;
                                    ''')
            except:
                await tr.rollback()
                raise
            else:
                await tr.commit()
        print("drop")

    async def delete_pool_connection(self):
        await self.drop_tables()
        await self.pool.close()
        if (await self.pool == None):
            print("Pool close")
