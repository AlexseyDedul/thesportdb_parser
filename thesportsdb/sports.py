"""
All Sports API Interactions on the free tier.

Get All Sports.
Get Sport Info.
Get TeamVsTeam Sports.
Get NONTeamVsTeam Sports.
"""
from __future__ import absolute_import

import asyncio

import thesportsdb.settings as TSD
from thesportsdb.request import make_request


async def allSports():
    return await make_request(TSD.ALL_SPORTS)
    ...
