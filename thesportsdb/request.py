"""
Module to Provide Functionality for Making requests to the API.
"""
from __future__ import absolute_import
import thesportsdb.settings as TSD
import aiohttp


def _make_url(endpoint: str) -> str:
    return TSD.BASE_URL + TSD.API_KEY + endpoint


async def make_request(endpoint: str, **kwargs):
    params = kwargs
    URL = _make_url(endpoint)
    async with aiohttp.ClientSession() as session:
        async with session.get(URL, params=params) as response:
            return await response.json()
