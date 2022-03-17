import asyncio

import asyncpg

from thesportsdb.events import leagueSeasonEvents
from thesportsdb.seasons import allSeason


async def get_events_by_league(leagues: list) -> list:
    list_events = []
    for i in leagues:
        try:
            seasons = await allSeason(str(i['idleague']))
            for s in seasons['seasons']:
                try:
                    await asyncio.sleep(2)
                    events = await leagueSeasonEvents(str(i['idleague']), s['strSeason'])
                    for e in events['events']:
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
    list_events = await get_events_by_league(leagues)
    if len(await get_event_ids_db(pool)) != len(list_events):
        async with pool.acquire() as conn:
            tr = conn.transaction()
            await tr.start()
            try:
                for e in list_events:
                    eventsExist = await conn.fetchrow(
                        'SELECT * FROM events WHERE idEvent=$1', e['idEvent'])
                    if eventsExist is None:
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

                        print("event insert")
            except:
                await tr.rollback()
                raise
            else:
                await tr.commit()
            print("insertEvents")
