import asyncpg

from app.thesportsdb_parser.utils import save_img_to_folder
from thesportsdb.leagues import leagueInfo
import logging


logger = logging.getLogger(__name__)


async def get_leagues_api(leagues: list) -> list:
    league_list = []
    try:
        for league in leagues['leagues']:
            try:
                league_info = await leagueInfo(league['idLeague'])
                if league_info is not None:
                    for l in league_info['leagues']:
                        league_list.append(l)
            except:
                logger.warning(f"Leagues info not found by league id: {league['idLeague']}")
                continue
    except TypeError:
        pass

    return league_list


async def insert_leagues(pool: asyncpg.pool.Pool, leagues: dict):
    league_list = await get_leagues_api(leagues)
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            for league in league_list:
                league_exist = await conn.fetchrow(
                    'SELECT idLeague FROM league WHERE idLeague=$1', int(league['idLeague']))
                if league_exist is None:
                    league_banner = await save_img_to_folder('/league_banner/', league["strBanner"],
                                                             league["strLeague"])
                    league_logo = await save_img_to_folder('/league_logo/', league["strLogo"], league["strLeague"])
                    league_badge = await save_img_to_folder('/league_badge/', league["strBadge"], league["strLeague"])
                    await conn.execute('''
                            INSERT INTO league(idLeague,
                                    strSport,
                                    strLeague,
                                    strLeagueAlternate,
                                    intDivision,
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
                        ''', int(league["idLeague"]),
                                       league["strSport"],
                                       league["strLeague"],
                                       league["strLeagueAlternate"],
                                       int(league["intDivision"]) if league["intDivision"] is not None else 0,
                                       league["strCurrentSeason"],
                                       int(league["intFormedYear"]) if league["intFormedYear"] is not None else 0,
                                       league["dateFirstEvent"],
                                       league["strCountry"],
                                       league["strWebsite"],
                                       league["strFacebook"],
                                       league["strTwitter"],
                                       league["strYoutube"],
                                       league["strRSS"],
                                       league["strDescriptionEN"],
                                       league["strDescriptionRU"],
                                       league["strTvRights"],
                                       league["strFanart1"],
                                       league["strFanart2"],
                                       league["strFanart3"],
                                       league["strFanart4"],
                                       league_banner if league_banner is not None else league["strBanner"],
                                       league_badge if league_badge is not None else league["strBadge"],
                                       league_logo if league_logo is not None else league["strLogo"],
                                       league["strPoster"],
                                       league["strTrophy"],
                                       league["strNaming"],
                                       league["strComplete"]
                                       )
                else:
                    await update_leagues(pool, league)

        except Exception as e:
            await tr.rollback()
            logger.error(f"Transaction rollback. League don`t be insert. Exception: {e}")
        else:
            await tr.commit()


async def update_leagues(pool: asyncpg.pool.Pool, league: dict):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        league_banner = await save_img_to_folder('/league_banner/', league["strBanner"], league["strLeague"])
        league_logo = await save_img_to_folder('/league_logo/', league["strLogo"], league["strLeague"])
        league_badge = await save_img_to_folder('/league_badge/', league["strBadge"], league["strLeague"])
        try:
            await conn.execute('''
                                UPDATE league
                                SET strSport=$1,
                                strLeague=$2,
                                strLeagueAlternate=$3,
                                intDivision=$4,
                                strCurrentSeason=$5,
                                intFormedYear=$6,
                                dateFirstEvent=$7,
                                strCountry=$8,
                                strWebsite=$9,
                                strFacebook=$10,
                                strTwitter=$11,
                                strYoutube=$12,
                                strRSS=$13,
                                strDescriptionEN=$14,
                                strDescriptionRU=$15,
                                strTvRights=$16,
                                strFanart1=$17,
                                strFanart2=$18,
                                strFanart3=$19,
                                strFanart4=$20,
                                strBanner=$21,
                                strBadge=$22,
                                strLogo=$23,
                                strPoster=$24,
                                strTrophy=$25,
                                strNaming=$26,
                                strComplete=$27
                                WHERE idleague=$28
                            ''',
                               league["strSport"],
                               league["strLeague"],
                               league["strLeagueAlternate"],
                               int(league["intDivision"]) if league["intDivision"] is not None else 0,
                               league["strCurrentSeason"],
                               int(league["intFormedYear"]) if league["intFormedYear"] is not None else 0,
                               league["dateFirstEvent"],
                               league["strCountry"],
                               league["strWebsite"],
                               league["strFacebook"],
                               league["strTwitter"],
                               league["strYoutube"],
                               league["strRSS"],
                               league["strDescriptionEN"],
                               league["strDescriptionRU"],
                               league["strTvRights"],
                               league["strFanart1"],
                               league["strFanart2"],
                               league["strFanart3"],
                               league["strFanart4"],
                               league_banner if league_banner is not None else league["strBanner"],
                               league_badge if league_badge is not None else league["strBadge"],
                               league_logo if league_logo is not None else league["strLogo"],
                               league["strPoster"],
                               league["strTrophy"],
                               league["strNaming"],
                               league["strComplete"],
                               int(league["idLeague"]))
        except Exception as e:
            await tr.rollback()
            logger.error(f"Transaction rollback. League don`t be update. Exception: {e}")
        else:
            await tr.commit()


async def get_leagues_ids_list(pool: asyncpg.pool.Pool) -> list:
    async with pool.acquire() as conn:
        leagues = await conn.fetch(
            'SELECT idLeague FROM league')
        if not leagues:
            return None
        return leagues
