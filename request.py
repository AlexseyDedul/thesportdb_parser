"""
Module to Provide Functionality for Making requests to the API.
"""
from __future__ import absolute_import
import settings as TSD
import requests
import aiohttp
import asyncio


def _make_url(endpoint: str) -> str:
    return TSD.BASE_URL + TSD.API_KEY + endpoint


async def make_request(endpoint: str, **kwargs):
    params = kwargs
    URL = _make_url(endpoint)
    print(URL)
    async with aiohttp.ClientSession() as session:
        async with session.get(URL, params=params) as response:
            print(await response.json())
            return response

    #
    # response = requests.get(URL, params=params)
    # return response.json()
