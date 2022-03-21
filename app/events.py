import asyncio

import asyncpg

from thesportsdb.events import leagueSeasonEvents, eventStatistics
from thesportsdb.seasons import allSeason


async def get_events_api(pool: asyncpg.pool.Pool, leagues: list) -> list:
    list_events = []
    async with pool.acquire() as conn:
        for i in leagues:
            try:
                seasons = await allSeason(str(i['idleague']))
                for s in seasons['seasons']:
                    try:
                        events = await leagueSeasonEvents(str(i['idleague']), s['strSeason'])
                        for e in events['events']:
                            team_home_exist = await conn.fetchrow('''
                                                            SELECT idTeam 
                                                            FROM team
                                                            WHERE idTeam=$1
                                                            ''', int(e['idHomeTeam']))
                            team_away_exist = await conn.fetchrow('''
                                                            SELECT idTeam 
                                                            FROM team
                                                            WHERE idTeam=$1
                                                            ''', int(e['idAwayTeam']))
                            if team_away_exist is not None and team_home_exist is not None:
                                list_events.append(e)
                    except:
                        continue
            except:
                continue
    return list_events


async def get_event_ids_db(pool) -> list:
    async with pool.acquire() as conn:
        return await conn.fetch('''
                                SELECT idevent FROM events;
                                ''')


async def insert_events(pool: asyncpg.pool.Pool, leagues: list):
    list_events = await get_events_api(pool, leagues)
    events_db = await get_event_ids_db(pool)
    if len(events_db) != len(list_events):
        async with pool.acquire() as conn:
            tr = conn.transaction()
            await tr.start()
            try:
                for e in list_events:
                    event_exist = await conn.fetchrow('''SELECT idevent 
                                                        FROM events 
                                                        WHERE idevent=$1''', int(e['idEvent']))
                    if event_exist is None:
                            print(f"event insert {int(e['idEvent'])}")
                            await conn.execute('''
                                    INSERT INTO events(
                                    idEvent,
                                    idLeague,
                                    strSport,
                                    strEvent,
                                    strEventAlternate,
                                    idHomeTeam,
                                    idAwayTeam,
                                    strFilename,
                                    strSeason,
                                    strDescriptionEN,
                                    intHomeScore,
                                    intRound,
                                    intAwayScore,
                                    intSpectators,
                                    strOfficial,
                                    strTimestamp,
                                    dateEvent,
                                    dateEventLocal,
                                    strTime,
                                    strTimeLocal,
                                    strTVStation,
                                    strResult,
                                    strVenue,
                                    strCountry,
                                    strCity,
                                    strPoster,
                                    strSquare,
                                    strFanart,
                                    strThumb,
                                    strBanner,
                                    strMap,
                                    strVideo,
                                    strStatus,
                                    strPostponed,
                                    strLocked
                                    ) VALUES(
                                    $1, $2, $3, $4, $5, $6, $7, $8,
                                    $9, $10, $11, $12, $13, $14, $15, $16,
                                    $17, $18, $19, $20, $21, $22, $23, $24,
                                    $25, $26, $27, $28, $29, $30, $31, $32,
                                    $33, $34, $35
                                    )
                                ''', int(e['idEvent']),
                                        int(e['idLeague']),
                                        e['strSport'],
                                        e['strEvent'],
                                        e['strEventAlternate'],
                                        int(e['idHomeTeam']),
                                        int(e['idAwayTeam']),
                                        e['strFilename'],
                                        e['strSeason'],
                                        e['strDescriptionEN'],
                                        int(e['intHomeScore']) if e['intHomeScore'] is not None else 0,
                                        int(e['intRound']) if e['intRound'] is not None else 0,
                                        int(e['intAwayScore']) if e['intAwayScore'] is not None else 0,
                                        int(e['intSpectators']) if e['intSpectators'] is not None else 0,
                                        e['strOfficial'],
                                        e['strTimestamp'],
                                        e['dateEvent'],
                                        e['dateEventLocal'],
                                        e['strTime'],
                                        e['strTimeLocal'],
                                        e['strTVStation'],
                                        e['strResult'],
                                        e['strVenue'],
                                        e['strCountry'],
                                        e['strCity'],
                                        e['strPoster'],
                                        e['strSquare'],
                                        e['strFanart'],
                                        e['strThumb'],
                                        e['strBanner'],
                                        e['strMap'],
                                        e['strVideo'],
                                        e['strStatus'],
                                        e['strPostponed'],
                                        e['strLocked'])
            except:
                await tr.rollback()
                raise
            else:
                await tr.commit()
            print("insertEvents")


async def get_event_stats_api(pool: asyncpg.pool.Pool):
    event_ids = await get_event_ids_db(pool)
    list_event_stats = []
    if len(event_ids) != 0:
        for event in event_ids:
            try:
                event_stats = await eventStatistics(str(event['idevent']))
                for event_stat in event_stats['eventstats']:
                    print(event_stat)
                    list_event_stats.append(event_stat)
            except:
                continue
    return list_event_stats


async def get_event_stats_count_db(pool: asyncpg.pool.Pool):
    async with pool.acquire() as conn:
        return await conn.fetchrow('''
                                    SELECT count(idStatistic)
                                    FROM eventStats
                                    ''')


async def insert_event_stats(pool: asyncpg.pool.Pool):
    list_event_stats = await get_event_stats_api(pool)
    count_db = await get_event_stats_count_db(pool)
    if count_db['count'] != len(list_event_stats):
        async with pool.acquire() as conn:
            tr = conn.transaction()
            await tr.start()
            try:
                for event_stat in list_event_stats:
                    event_stat_exist = await conn.fetchrow('''
                                                            SELECT idStatistic
                                                            FROM eventStats
                                                            WHERE idStatistic=$1
                                                            ''', int(event_stat['idStatistic']))
                    if event_stat_exist is None:
                        await conn.execute(''' INSERT INTO eventStats(
                                            idStatistic,
                                            idEvent,
                                            strEvent,
                                            strStat,
                                            intHome,
                                            intAway)
                                            VALUES($1, $2, $3, $4, $5, $6)
                                            ''',
                                            int(event_stat['idStatistic']),
                                            int(event_stat['idEvent']),
                                            event_stat['strEvent'],
                                            event_stat['strStat'],
                                            int(event_stat['intHome']),
                                            int(event_stat['intAway']))
                        print("insert eventStats")
            except:
                await tr.rollback()
                raise
            else:
                await tr.commit()
