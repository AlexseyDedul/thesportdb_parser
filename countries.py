"""
All Countries API interactions.

Get all Countries.
Get Sports for Country.
"""
from __future__ import absolute_import
import settings as TSD
from request import make_request


def allCountries():
    return make_request(TSD.ALL_COUNTRIES)
    ...