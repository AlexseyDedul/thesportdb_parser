import asyncpg

from thesportsdb.players import playersHonours
import logging


logger = logging.getLogger(__name__)


async def get_honours_team_api(pool: asyncpg.pool.Pool, players: list) -> list:
    list_honours = []
    async with pool.acquire() as conn:
        for p in players:
            honours_team = await playersHonours(str(p['idplayer']))
            try:
                if honours_team is not None:
                    for honour in honours_team['honors']:
                        team_exist = await conn.fetchrow('''
                                                        SELECT idTeam 
                                                        FROM team
                                                        WHERE idTeam=$1
                                                        ''', int(honour['idTeam']))
                        if team_exist is not None:
                            list_honours.append(honour)
            except:
                logger.warning(f"Honours team not found by id player: {p['idplayer']}")
                continue
    return list_honours


async def update_honours_team(pool: asyncpg.pool.Pool, honours: dict):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            await conn.execute('''
                                UPDATE honoursTeam
                                SET idPlayer=$1,
                                idTeam=$2,
                                strSport=$3,
                                strPlayer=$4,
                                strTeam=$5,
                                strHonour=$6,
                                strSeason=$7,
                                intChecked=$8
                                WHERE idHonoursTeam=$9
                                ''',
                               int(honours['idPlayer']),
                               int(honours['idTeam']),
                               honours['strSport'],
                               honours['strPlayer'],
                               honours['strTeam'],
                               honours['strHonour'],
                               honours['strSeason'],
                               honours['intChecked'],
                               int(honours['id']))
        except Exception as e:
            await tr.rollback()
            logger.error(f"Transaction rollback. Honours team don`t be update. Exception: {e}")
        else:
            await tr.commit()


async def insert_honours_teams(pool: asyncpg.pool.Pool, players: list):
    honours_teams = await get_honours_team_api(pool, players)
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            for honours in honours_teams:
                honour_team_exist = await conn.fetchrow('''
                                        SELECT idHonoursTeam
                                        FROM honoursTeam
                                        WHERE idHonoursTeam=$1
                                        ''', int(honours['id']))
                if honour_team_exist is None:
                    await conn.execute('''
                                        INSERT INTO honoursTeam(
                                        idHonoursTeam,
                                        idPlayer,
                                        idTeam,
                                        strSport,
                                        strPlayer,
                                        strTeam,
                                        strHonour,
                                        strSeason,
                                        intChecked)
                                        VALUES(
                                        $1, $2, $3,
                                        $4, $5, $6,
                                        $7, $8, $9
                                        )
                                        ''', int(honours['id']),
                                       int(honours['idPlayer']),
                                       int(honours['idTeam']),
                                       honours['strSport'],
                                       honours['strPlayer'],
                                       honours['strTeam'],
                                       honours['strHonour'],
                                       honours['strSeason'],
                                       honours['intChecked'])
                else:
                    await update_honours_team(pool, honours)
        except Exception as e:
            await tr.rollback()
            logger.error(f"Transaction rollback. Honours team don`t be insert. Exception: {e}")
        else:
            await tr.commit()
