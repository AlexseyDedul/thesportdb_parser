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


async def get_tables_list_db(pool: asyncpg.pool.Pool):
    async with pool.acquire() as conn:
        return await conn.fetchrow('''
                                SELECT count(*) FROM tables;
                                ''')


async def insert_tables(pool: asyncpg.pool.Pool, leagues: list):
    tables = await get_tables_api(leagues)
    tables_db = await get_tables_list_db(pool)
    if tables_db['count'] != len(tables):
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
                                           int(t['intGoalDifference']) if t[
                                                                              'intGoalDifference'] is not None else 0,
                                           int(t['intPoints']) if t['intPoints'] is not None else 0,
                                           t['dateUpdated'])
                        print(f"tables insert {len(tables)}")

            except:
                print('tables rollback')
                await tr.rollback()
                raise
            else:
                await tr.commit()
            print("insert_tables")
    else:
        print('doesn`t insert tables')
