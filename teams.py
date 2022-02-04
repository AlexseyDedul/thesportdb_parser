"""
All Teams API Interactions on the free tier.

Get teams for League.
Get team Info by ID.
"""
from __future__ import absolute_import
import settings as TSD
from request import make_request


def leagueTeams(league_id: str):
    return make_request(TSD.LEAGUE_TEAMS, id=league_id)
    ...


def teamInfo(team_id: str):
    return make_request(TSD.TEAM, id=team_id)
    ...


def searchTeamsByName(team_name: str):
    return make_request(TSD.SEARCH_TEAMS, t=team_name)


def equipment(team_id: str):
    return make_request(TSD.LOOKUP_EQUIPMENT, id=team_id)
