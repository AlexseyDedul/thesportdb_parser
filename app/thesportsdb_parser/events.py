import asyncpg

from app.thesportsdb_parser.leagues import get_leagues_ids_list
from thesportsdb.events import leagueSeasonEvents, eventStatistics
from thesportsdb.seasons import allSeason


async def get_events_api(pool: asyncpg.pool.Pool) -> list:
    leagues = await get_leagues_ids_list(pool)
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


async def insert_events(pool: asyncpg.pool.Pool, list_events: list = None):
    if list_events is None:
        list_events = await get_events_api(pool)
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            for e in list_events:
                event_exist = await conn.fetchrow('''SELECT idevent 
                                                    FROM events 
                                                    WHERE idevent=$1''', int(e['idEvent']))
                if event_exist is None:
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
                else:
                    await update_event(pool, e)
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()
        print("insertEvents")


async def update_event(pool: asyncpg.pool.Pool, event: dict):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            await conn.execute('''
                                UPDATE events
                                SET idLeague=$1,
                                strSport=$2,
                                strEvent=$3,
                                strEventAlternate=$4,
                                idHomeTeam=$5,
                                idAwayTeam=$6,
                                strFilename=$7,
                                strSeason=$8,
                                strDescriptionEN=$9,
                                intHomeScore=$10,
                                intRound=$11,
                                intAwayScore=$12,
                                intSpectators=$13,
                                strOfficial=$14,
                                strTimestamp=$15,
                                dateEvent=$16,
                                dateEventLocal=$17,
                                strTime=$18,
                                strTimeLocal=$19,
                                strTVStation=$20,
                                strResult=$21,
                                strVenue=$22,
                                strCountry=$23,
                                strCity=$24,
                                strPoster=$25,
                                strSquare=$26,
                                strFanart=$27,
                                strThumb=$28,
                                strBanner=$29,
                                strMap=$30,
                                strVideo=$31,
                                strStatus=$32,
                                strPostponed=$33,
                                strLocked=$34
                                WHERE idevent=$35
                        ''',
                               int(event['idLeague']),
                               event['strSport'],
                               event['strEvent'],
                               event['strEventAlternate'],
                               int(event['idHomeTeam']),
                               int(event['idAwayTeam']),
                               event['strFilename'],
                               event['strSeason'],
                               event['strDescriptionEN'],
                               int(event['intHomeScore']) if event['intHomeScore'] is not None else 0,
                               int(event['intRound']) if event['intRound'] is not None else 0,
                               int(event['intAwayScore']) if event['intAwayScore'] is not None else 0,
                               int(event['intSpectators']) if event['intSpectators'] is not None else 0,
                               event['strOfficial'],
                               event['strTimestamp'],
                               event['dateEvent'],
                               event['dateEventLocal'],
                               event['strTime'],
                               event['strTimeLocal'],
                               event['strTVStation'],
                               event['strResult'],
                               event['strVenue'],
                               event['strCountry'],
                               event['strCity'],
                               event['strPoster'],
                               event['strSquare'],
                               event['strFanart'],
                               event['strThumb'],
                               event['strBanner'],
                               event['strMap'],
                               event['strVideo'],
                               event['strStatus'],
                               event['strPostponed'],
                               event['strLocked'],
                               int(event['idEvent']), )
            print('updated event')
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()


async def get_event_stats_api(event_ids: list):
    list_event_stats = []
    if len(event_ids) != 0:
        for event in event_ids:
            try:
                event_stats = await eventStatistics(str(event['idevent']))
                for event_stat in event_stats['eventstats']:
                    list_event_stats.append(event_stat)
            except:
                continue
    return list_event_stats


async def update_event_stats(pool: asyncpg.pool.Pool, event_stat: dict):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            await conn.execute(''' 
                                UPDATE eventStats
                                SET idEvent=$1,
                                    strEvent=$2,
                                    strStat=$3,
                                    intHome=$4,
                                    intAway=$5
                                    WHERE idStatistic=$6
                                    ''',
                               int(event_stat['idEvent']),
                               event_stat['strEvent'],
                               event_stat['strStat'],
                               int(event_stat['intHome']),
                               int(event_stat['intAway']),
                               int(event_stat['idStatistic']))
            print("insert eventStats")
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()


async def insert_event_stats(pool: asyncpg.pool.Pool, event_ids: list = None):
    if event_ids is None:
        event_ids = await get_event_ids_db(pool)
    list_event_stats = await get_event_stats_api(event_ids)
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
                else:
                    await update_event_stats(pool, event_stat)
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()
