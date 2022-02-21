"""
All Players related API interactions.

Get All Players in a Team by Team Id.
Get Player Details by Id. ?
Get Player Honours by Player Id.
Get Player Former Teams by Player Id
Get Player Contracts by Player Id
"""
from __future__ import absolute_import
import thesportsdb.settings as TSD
from thesportsdb.request import make_request


async def teamPlayers(team_id: str):
    return await make_request(TSD.TEAM_PLAYERS, id=team_id)
    ...


async def playerDetails(player_id: str):
    return await make_request(TSD.PLAYER, id=player_id)
    ...


async def searchPlayersByName(player_name: str):
    return await make_request(TSD.SEARCH_PLAYERS, p=player_name)
    ...


async def playersHonours(player_id: str):
    return await make_request(TSD.LOOKUP_HONORS, id=player_id)
    ...


async def playersFormerTeam(player_id: str):
    return await make_request(TSD.LOOKUP_FORMER_TEAMS, id=player_id)
    ...


async def playersContracts(player_id: str):
    return await make_request(TSD.LOOKUP_CONTRACTS, id=player_id)
    ...
