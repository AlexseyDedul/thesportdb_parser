import asyncpg
import logging


logger = logging.getLogger(__name__)


class Database:
    def __init__(self, user: str, password: str, database: str, host: str):
        if user is not None:
            self.user = user
        else:
            logger.error("User db not found.")

        if password is not None:
            self.password = password
        else:
            logger.error("Password db not found.")

        if database is not None:
            self.database = database
        else:
            logger.error("Database db not found.")

        if host is not None:
            self.host = host
        else:
            logger.error("Host db not found.")

        self.pool = None

    async def create_tables(self):
        async with self.pool.acquire() as conn:
            tr = conn.transaction()
            await tr.start()
            try:
                await conn.execute('''
                                    CREATE TABLE IF NOT EXISTS sports(
                                        idSport bigint PRIMARY KEY,
                                        strSport text,
                                        strFormat text
                                    );
                                    CREATE TABLE IF NOT EXISTS countries(
                                        id serial PRIMARY KEY,
                                        name_en text
                                    );
                                    CREATE TABLE IF NOT EXISTS league(
                                        idLeague bigint PRIMARY KEY,
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
                                        idTeam bigint PRIMARY KEY,
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
                                        idLeague bigint REFERENCES league (idLeague),
                                        idTeam bigint REFERENCES team (idTeam)
                                    );                                    
                                    CREATE TABLE IF NOT EXISTS events(
                                        idEvent bigint PRIMARY KEY,
                                        idLeague integer REFERENCES league (idLeague),
                                        strSport text,
                                        strEvent text,
                                        strEventAlternate text,
                                        idHomeTeam bigint REFERENCES team (idTeam),
                                        idAwayTeam bigint REFERENCES team (idTeam),
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
                                        idStanding bigint PRIMARY KEY,
                                        intRank integer,
                                        idTeam bigint REFERENCES team (idTeam),
                                        idLeague bigint REFERENCES league (idLeague),
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
                                        idPlayer bigint PRIMARY KEY,
                                        strNationality text,
                                        strPlayer text,
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
                                        idPlayer bigint REFERENCES player (idPlayer),
                                        idTeam bigint REFERENCES team (idTeam)
                                    );
                                    CREATE TABLE IF NOT EXISTS contract(
                                        idContract bigint PRIMARY KEY,
                                        idPlayer bigint REFERENCES player (idPlayer),
                                        idTeam bigint REFERENCES team (idTeam),
                                        strSport text,
                                        strPlayer text,
                                        strTeam text,
                                        strTeamBadge text,
                                        strYearStart text,
                                        strYearEnd text,
                                        strWage text
                                    );
                                    CREATE TABLE IF NOT EXISTS formerTeam(
                                        idFormerTeam bigint PRIMARY KEY,
                                        idPlayer bigint REFERENCES player (idPlayer),
                                        idTeam bigint REFERENCES team (idTeam),
                                        strSport text,
                                        strPlayer text,
                                        strFormerTeam text,
                                        strMoveType text,
                                        strTeamBadge text,
                                        strJoined text,
                                        strDeparted text
                                    );
                                    CREATE TABLE IF NOT EXISTS honoursTeam(
                                        idHonoursTeam bigint PRIMARY KEY,
                                        idPlayer bigint REFERENCES player (idPlayer),
                                        idTeam bigint REFERENCES team (idTeam),
                                        strSport text,
                                        strPlayer text,
                                        strTeam text,
                                        strHonour text,
                                        strSeason text,
                                        intChecked text
                                    );
                                    CREATE TABLE IF NOT EXISTS eventStats(
                                        idStatistic bigint PRIMARY KEY,
                                        idEvent bigint REFERENCES events (idEvent),
                                        strEvent text,
                                        strStat text,
                                        intHome integer,
                                        intAway integer
                                    ); 
                                    CREATE TABLE IF NOT EXISTS channel(
                                        idChannel bigint PRIMARY KEY,
                                        strChannel text
                                    ); 
                                    CREATE TABLE IF NOT EXISTS eventTV(
                                        id bigint PRIMARY KEY,
                                        idEvent bigint REFERENCES events (idEvent),
                                        idChannel integer,
                                        strCountry text,
                                        strLogo text,
                                        strSeason text,
                                        strTime text,
                                        dateEvent text,
                                        strTimeStamp text
                                    ); 
                                    CREATE TABLE IF NOT EXISTS timeline(
                                        idTimeline bigint PRIMARY KEY,
                                        idEvent bigint REFERENCES events (idEvent),
                                        idPlayer bigint REFERENCES player (idPlayer),
                                        idTeam bigint REFERENCES team (idTeam),
                                        strTimeline text,
                                        strTimelineDetail text,
                                        strHome text,
                                        strEvent text,
                                        strCountry text,
                                        idAssist integer,
                                        strAssist text,
                                        intTime text,
                                        strComment text,
                                        dateEvent text,
                                        strSeason text
                                    );  
                                    CREATE TABLE IF NOT EXISTS lineup(
                                        idLineup bigint PRIMARY KEY,
                                        idEvent bigint REFERENCES events (idEvent),
                                        idPlayer bigint REFERENCES player (idPlayer),
                                        idTeam bigint REFERENCES team (idTeam),
                                        strEvent text,
                                        strPosition text,
                                        strPositionShort text,
                                        strFormation integer,
                                        strHome text,
                                        strSubstitute text,
                                        intSquadNumber text,
                                        strCountry text,
                                        strSeason text
                                    ); 
                                ''')
            except:
                await tr.rollback()
                raise
            else:
                await tr.commit()
                logger.info("Tables created.")

    async def get_pool_connection(self) -> asyncpg.pool.Pool:
        try:
            self.pool = await asyncpg.create_pool(user=self.user,
                                             password=self.password,
                                             database=self.database,
                                             host=self.host)
            return self.pool
        except:
            logger.error("Pool connection doesn`t create. Check correctly connection data in .env file.")

    async def drop_tables(self):
        async with self.pool.acquire() as conn:
            tr = conn.transaction()
            await tr.start()
            try:
                await conn.execute('''
                                    DROP TABLE IF EXISTS contract;
                                    DROP TABLE IF EXISTS countries;
                                    DROP TABLE IF EXISTS events CASCADE;
                                    DROP TABLE IF EXISTS sports CASCADE;
                                    DROP TABLE IF EXISTS league CASCADE;
                                    DROP TABLE IF EXISTS team CASCADE;
                                    DROP TABLE IF EXISTS eventStats CASCADE;
                                    DROP TABLE IF EXISTS formerteam;
                                    DROP TABLE IF EXISTS channel CASCADE;
                                    DROP TABLE IF EXISTS eventTV CASCADE;
                                    DROP TABLE IF EXISTS timeline;
                                    DROP TABLE IF EXISTS lineup;
                                    DROP TABLE IF EXISTS honoursteam;
                                    DROP TABLE IF EXISTS playerteam;
                                    DROP TABLE IF EXISTS player CASCADE;
                                    
                                    DROP TABLE IF EXISTS tables CASCADE;
                                    DROP TABLE IF EXISTS teamleague CASCADE;
                                    ''')
            except Exception as e:
                await tr.rollback()
                logger.error(f"Error: {e}")
            else:
                await tr.commit()
        logger.info("Drop tables")

    async def delete_pool_connection(self):
        await self.pool.close()
        if await self.pool is None:
            logger.info("Pool close")
