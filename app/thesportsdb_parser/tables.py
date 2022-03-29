import asyncpg

from thesportsdb.leagues import leagueSeasonTable
from thesportsdb.seasons import allSeason


async def get_tables_api(leagues: list) -> list:
    tables = []
    for i in leagues:
        try:
            seasons = await allSeason(str(i['idleague']))
            for s in seasons['seasons']:
                try:
                    table = await leagueSeasonTable(str(i['idleague']), s['strSeason'])
                    for t in table['table']:
                        tables.append(t)
                except:
                    continue
        except:
            continue
    return tables


async def insert_tables(pool: asyncpg.pool.Pool, leagues: list):
    tables = await get_tables_api(leagues)
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            for t in tables:
                tableExist = await conn.fetchrow('''
                                                    SELECT idStanding
                                                    FROM tables
                                                    WHERE idStanding=$1
                                                    ''', int(t['idStanding']))
                if tableExist is None:
                    await conn.execute('''
                                    INSERT INTO tables(
                                    idStanding,
                                    intRank,
                                    idTeam ,
                                    idLeague,
                                    strSeason,
                                    strForm,
                                    strDescription,
                                    intPlayed,
                                    intWin,
                                    intLoss,
                                    intDraw,
                                    intGoalsFor,
                                    intGoalsAgainst,
                                    intGoalDifference,
                                    intPoints,
                                    dateUpdated
                                    )                                                 
                                    VALUES($1, $2, $3, 
                                    $4, $5, $6, 
                                    $7, $8, $9, 
                                    $10, $11, $12, 
                                    $13, $14, $15, $16)
                                ''', int(t['idStanding']),
                                       int(t['intRank']) if t['intRank'] is not None else 0,
                                       int(t['idTeam']),
                                       int(t['idLeague']),
                                       t['strSeason'],
                                       t['strForm'],
                                       t['strDescription'],
                                       int(t['intPlayed']) if t['intPlayed'] is not None else 0,
                                       int(t['intWin']) if t['intWin'] is not None else 0,
                                       int(t['intLoss']) if t['intLoss'] is not None else 0,
                                       int(t['intDraw']) if t['intDraw'] is not None else 0,
                                       int(t['intGoalsFor']) if t['intGoalsFor'] is not None else 0,
                                       int(t['intGoalsAgainst']) if t['intGoalsAgainst'] is not None else 0,
                                       int(t['intGoalDifference']) if t['intGoalDifference'] is not None else 0,
                                       int(t['intPoints']) if t['intPoints'] is not None else 0,
                                       t['dateUpdated'])
                    print(f"tables insert {len(tables)}")
                else:
                    await update_tables(pool, t)
        except:
            print('tables rollback')
            await tr.rollback()
            raise
        else:
            await tr.commit()
        print("insert_tables")


async def update_tables(pool: asyncpg.pool.Pool, table: dict):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            await conn.execute('''
                                UPDATE tables
                                SET 
                                intRank=$1,
                                idTeam=$2 ,
                                idLeague=$3,
                                strSeason=$4,
                                strForm=$5,
                                strDescription=$6,
                                intPlayed=$7,
                                intWin=$8,
                                intLoss=$9,
                                intDraw=$10,
                                intGoalsFor=$11,
                                intGoalsAgainst=$12,
                                intGoalDifference=$13,
                                intPoints=$14,
                                dateUpdated=$15
                                WHERE idStanding=$16
                            ''',
                               int(table['intRank']) if table['intRank'] is not None else 0,
                               int(table['idTeam']),
                               int(table['idLeague']),
                               table['strSeason'],
                               table['strForm'],
                               table['strDescription'],
                               int(table['intPlayed']) if table['intPlayed'] is not None else 0,
                               int(table['intWin']) if table['intWin'] is not None else 0,
                               int(table['intLoss']) if table['intLoss'] is not None else 0,
                               int(table['intDraw']) if table['intDraw'] is not None else 0,
                               int(table['intGoalsFor']) if table['intGoalsFor'] is not None else 0,
                               int(table['intGoalsAgainst']) if table['intGoalsAgainst'] is not None else 0,
                               int(table['intGoalDifference']) if table['intGoalDifference'] is not None else 0,
                               int(table['intPoints']) if table['intPoints'] is not None else 0,
                               table['dateUpdated'],
                               int(table['idStanding']))
            print('table updated')
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()
