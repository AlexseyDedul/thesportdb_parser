"""
All Sports API Interactions on the free tier.

Get All Sports.
Get Sport Info.
Get TeamVsTeam Sports.
Get NONTeamVsTeam Sports.
"""
from __future__ import absolute_import
import settings as TSD
from request import make_request


def allSports():
    return make_request(TSD.ALL_SPORTS)
    ...
