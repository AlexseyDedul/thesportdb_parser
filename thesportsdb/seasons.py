"""
All League related API interactions on the free tier.

Get All Seasons by League.
"""
from __future__ import absolute_import

import thesportsdb.settings as TSD
from thesportsdb.request import make_request


async def allSeason():
    return await make_request(TSD.SEASONS)
    ...