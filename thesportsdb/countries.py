"""
All Countries API interactions.

Get all Countries.
Get Sports for Country.
"""
from __future__ import absolute_import

import asyncio

import thesportsdb.settings as TSD
from thesportsdb.request import make_request


async def allCountries():
    return await make_request(TSD.ALL_COUNTRIES)
    ...