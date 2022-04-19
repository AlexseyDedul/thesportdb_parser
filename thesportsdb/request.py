"""
Module to Provide Functionality for Making requests to the API.
"""
from __future__ import absolute_import

import asyncio

import thesportsdb.settings as TSD
import aiohttp


def _make_url(endpoint: str) -> str:
    return TSD.BASE_URL + TSD.API_KEY + endpoint


async def make_request(endpoint: str, **kwargs):
    params = kwargs
    URL = _make_url(endpoint)
    async with aiohttp.ClientSession() as session:
        await asyncio.sleep(2)
        async with session.get(URL, params=params) as response:
            print(response)
            if response.content_type == 'application/json' and response.status == 200:
                return await response.json()
            else:
                return None
