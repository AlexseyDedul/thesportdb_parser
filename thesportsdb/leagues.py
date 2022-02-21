"""
All League related API interactions on the free tier.

Get All Leagues.
Get League Info.
Get Leagues for Sport.
Get Table for League at a particular Season.
"""
from __future__ import absolute_import

import asyncio

import thesportsdb.settings as TSD
from thesportsdb.request import make_request


async def allLeagues():
    return await make_request(TSD.ALL_LEAGUES)
    ...


async def leagueSeasonTable(league_id: str, season: str):
    return await make_request(TSD.LEAGUE_SEASON_TABLE, l=league_id, s=season)
    ...


async def leagueInfo(league_id: str):
    return await make_request(TSD.LEAGUE, id=league_id)
    ...

