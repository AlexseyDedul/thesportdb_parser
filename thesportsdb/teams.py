"""
All Teams API Interactions on the free tier.

Get teams for League.
Get team Info by ID.
"""
from __future__ import absolute_import
import thesportsdb.settings as TSD
from thesportsdb.request import make_request


async def leagueTeams(league_id: str):
    return await make_request(TSD.LEAGUE_TEAMS, id=league_id)
    ...


async def teamInfo(team_id: str):
    return await make_request(TSD.TEAM, id=team_id)
    ...


async def searchTeamsByName(team_name: str):
    return await make_request(TSD.SEARCH_TEAMS, t=team_name)


async def equipment(team_id: str):
    return await make_request(TSD.LOOKUP_EQUIPMENT, id=team_id)
