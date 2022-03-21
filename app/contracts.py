import asyncio

import asyncpg.pool

from thesportsdb.players import playersContracts


async def get_contracts_api(players: list) -> list:
    list_contracts = []
    for p in players:
        contract = await playersContracts(str(p['idplayer']))
        try:
            for contr in contract['contracts']:
                list_contracts.append(contr)
        except:
            continue
    return list_contracts


async def get_contracts_db(pool: asyncpg.pool.Pool):
    async with pool.acquire() as conn:
        count_contracts = await conn.fetchrow('''
                                                SELECT count(*)
                                                FROM contract
                                                ''')
    return count_contracts['count']


async def insert_contracts(pool: asyncpg.pool.Pool, players: list):
    contracts = await get_contracts_api(players)
    if await get_contracts_db(pool) != len(contracts):
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
                        print('contract insert')
            except:
                await tr.rollback()
                raise
            else:
                await tr.commit()
