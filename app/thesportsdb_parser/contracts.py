import asyncpg.pool

from thesportsdb.players import playersContracts
import logging


logger = logging.getLogger(__name__)


async def get_contracts_api(players: list) -> list:
    list_contracts = []
    for p in players:
        contracts = await playersContracts(str(p['idplayer']))
        try:
            for contract in contracts['contracts']:
                list_contracts.append(contract)
        except:
            logger.warning(f"Contract by league id {p['idplayer']} not found.")
            continue
    return list_contracts


async def update_contracts(pool: asyncpg.pool.Pool, contract: dict):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            await conn.execute('''
                                UPDATE contract
                                SET idPlayer=$1,
                                    idTeam=$2,
                                    strSport=$3,
                                    strPlayer=$4,
                                    strTeam=$5,
                                    strTeamBadge=$6,
                                    strYearStart=$7,
                                    strYearEnd=$8,
                                    strWage=$9
                                WHERE idcontract=$10
                                ''', int(contract['idPlayer']),
                               int(contract['idTeam']),
                               contract['strSport'],
                               contract['strPlayer'],
                               contract['strTeam'],
                               contract['strTeamBadge'],
                               contract['strYearStart'],
                               contract['strYearEnd'],
                               contract['strWage'],
                               int(contract['id']))
        except Exception as e:
            await tr.rollback()
            logger.error(f"Transaction rollback. Contracts don`t be update. Exception: {e}")
        else:
            await tr.commit()


async def insert_contracts(pool: asyncpg.pool.Pool, players: list):
    contracts = await get_contracts_api(players)
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            for contract in contracts:
                contract_exist = await conn.fetchrow('''
                                        SELECT idcontract
                                        FROM contract
                                        WHERE idcontract=$1
                                        ''', int(contract['id']))
                if contract_exist is None:
                    await conn.execute('''
                                        INSERT INTO contract(idContract,
                                        idPlayer,
                                        idTeam,
                                        strSport,
                                        strPlayer,
                                        strTeam,
                                        strTeamBadge,
                                        strYearStart,
                                        strYearEnd,
                                        strWage)
                                        VALUES(
                                        $1, $2, $3,
                                        $4, $5, $6,
                                        $7, $8, $9,
                                        $10
                                        )
                                        ''', int(contract['id']),
                                       int(contract['idPlayer']),
                                       int(contract['idTeam']),
                                       contract['strSport'],
                                       contract['strPlayer'],
                                       contract['strTeam'],
                                       contract['strTeamBadge'],
                                       contract['strYearStart'],
                                       contract['strYearEnd'],
                                       contract['strWage'])
                else:
                    await update_contracts(pool, contract)
        except Exception as e:
            await tr.rollback()
            logger.error(f"Transaction rollback. Contracts don`t be insert. Exception: {e}")
        else:
            await tr.commit()
