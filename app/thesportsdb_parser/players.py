import asyncpg

from app.thesportsdb_parser.teams import get_teams_ids_list
from app.thesportsdb_parser.utils import save_img_to_folder
from thesportsdb.players import teamPlayers, playerDetails
import logging


logger = logging.getLogger(__name__)


async def get_players_api(pool: asyncpg.pool.Pool) -> list:
    list_players = []
    teams = await get_teams_ids_list(pool)
    for t in teams:
        try:
            player = await teamPlayers(str(t['idteam']))
            if player is not None:
                for p in player['player']:
                    list_players.append(p)
        except Exception as e:
            logger.warning(f"PLayer not found by id team: {t['idteam']}. Exception: {e}")
            continue
    return list_players


async def get_list_players_db(pool: asyncpg.pool.Pool) -> list:
    async with pool.acquire() as conn:
        return await conn.fetch('''
                                SELECT idPlayer 
                                FROM player;
                                ''')


async def get_player_from_db_by_id(pool: asyncpg.pool.Pool, player_id: int):
    async with pool.acquire() as conn:
        return await conn.fetch(f'''
                                SELECT idPlayer 
                                FROM player
                                WHERE idPlayer={player_id}
                                ''')


async def check_player_in_db(pool: asyncpg.pool.Pool, player_id: int):
    player = await get_player_from_db_by_id(pool, player_id)
    if not player:
        await get_player_api_by_id(pool, player_id)


async def get_player_api_by_id(pool: asyncpg.pool.Pool, player_id: int):
    list_player = []
    try:
        players = await playerDetails(str(player_id))
        if players is not None:
            for player in players['players']:
                list_player.append(player)
        await insert_players(pool, list_player)
    except Exception as e:
        logger.warning(f"PLayer not found by id player: {player_id}. Exception: {e}")
        return None


async def update_player(pool: asyncpg.pool.Pool, player: dict):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            player_logo = await save_img_to_folder('/player_logo/', player['strThumb'], player['strPlayer'])
            await conn.execute('''
                                UPDATE player
                                SET strNationality=$1,
                                    strPlayer=$2,
                                    strTeam=$3,
                                    strSport=$4,
                                    dateBorn=$5,
                                    strNumber=$6,
                                    dateSigned=$7,
                                    strSigning=$8,
                                    strWage=$9,
                                    strOutfitter=$10,
                                    strKit=$11,
                                    strAgent=$12,
                                    strBirthLocation=$13,
                                    strDescriptionEN=$14,
                                    strDescriptionRU=$15,
                                    strGender=$16,
                                    strSide=$17,
                                    strPosition=$18,
                                    strFacebook=$19,
                                    strWebsite=$20,
                                    strTwitter=$21,
                                    strInstagram=$22,
                                    strYoutube=$23,
                                    strHeight=$24,
                                    strWeight=$25,
                                    strThumb=$26,
                                    strFanart1=$27,
                                    strFanart2=$28,
                                    strFanart3=$29,
                                    strFanart4=$30
                                WHERE idplayer=$31
                            ''',
                               player['strNationality'],
                               player['strPlayer'],
                               player['strTeam'],
                               player['strSport'],
                               player['dateBorn'],
                               player['strNumber'],
                               player['dateSigned'],
                               player['strSigning'],
                               player['strWage'],
                               player['strOutfitter'],
                               player['strKit'],
                               player['strAgent'],
                               player['strBirthLocation'],
                               player['strDescriptionEN'],
                               player['strDescriptionRU'],
                               player['strGender'],
                               player['strSide'],
                               player['strPosition'],
                               player['strFacebook'],
                               player['strWebsite'],
                               player['strTwitter'],
                               player['strInstagram'],
                               player['strYoutube'],
                               player['strHeight'],
                               player['strWeight'],
                               player_logo if player_logo is not None else player['strThumb'],
                               player['strFanart1'],
                               player['strFanart2'],
                               player['strFanart3'],
                               player['strFanart4'],
                               int(player['idPlayer']))
        except Exception as e:
            await tr.rollback()
            logger.error(f"Transaction rollback. Player don`t be update. Exception: {e}")
        else:
            await tr.commit()


async def insert_players(pool: asyncpg.pool.Pool, list_player: list = None):
    if not list_player:
        players = await get_players_api(pool)
    else:
        players = list_player
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            for p in players:
                player_exist = await conn.fetchrow('''
                                                    SELECT * FROM player WHERE idPlayer=$1
                                                    ''', int(p['idPlayer']))
                if player_exist is None:
                    player_logo = await save_img_to_folder('/player_logo/', p['strThumb'], p['strPlayer'])
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
                                   player_logo if player_logo is not None else p['strThumb'],
                                   p['strFanart1'],
                                   p['strFanart2'],
                                   p['strFanart3'],
                                   p['strFanart4'])
                else:
                    await update_player(pool, p)
        except Exception as e:
            await tr.rollback()
            logger.error(f"Transaction rollback. Player don`t be insert. Exception: {e}")
        else:
            await tr.commit()
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
        except Exception as e:
            await tr.rollback()
            logger.error(f"Transaction rollback. TeamPlayer table don`t be insert. Exception: {e}")
        else:
            await tr.commit()
