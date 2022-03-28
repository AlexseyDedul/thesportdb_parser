import asyncio

import asyncpg.pool

from thesportsdb.players import playersContracts


async def get_contracts_api(players: list) -> list:
    list_contracts = []
    for p in players:
        contracts = await playersContracts(str(p['idplayer']))
        try:
            for contract in contracts['contracts']:
                list_contracts.append(contract)
        except:
            continue
    return list_contracts

#
# async def get_contracts_db(pool: asyncpg.pool.Pool):
#     async with pool.acquire() as conn:
#         count_contracts = await conn.fetchrow('''
#                                                 SELECT count(*)
#                                                 FROM contract
#                                                 ''')
#     return count_contracts['count']


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
            print(f"update contract: {int(contract['id'])}")

        except:
            await tr.rollback()
            raise
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
                    print('contract insert')
                else:
                    await update_contracts(pool, contract)
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()
