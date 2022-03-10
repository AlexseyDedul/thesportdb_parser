import asyncpg

from thesportsdb.events import leagueSeasonEvents
from thesportsdb.seasons import allSeason


async def insert_events(pool: asyncpg.pool.Pool, leagues: list):
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            for i in leagues:
                try:
                    seasons = await allSeason(i['idleague'])
                    for s in seasons['seasons']:
                        try:
                            events = await leagueSeasonEvents(i['idleague'], s['strSeason'])
                            for e in events['events']:
                                # eventsExist = await conn.fetch(
                                #     'SELECT * FROM events WHERE idEvent=$1', t['idEvent'])
                                # if(eventsExist == []):
                                await conn.execute('''
                                                    INSERT INTO events(idEvent, strEvent) VALUES($1, $2)
                                                ''', e['idEvent'], e['strEvent'])
                        except:
                            continue
                    print("league insert")
                except:
                    continue
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()
        print("insertEvents")

