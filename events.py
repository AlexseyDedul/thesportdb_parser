"""
All Event API interactions on the free tier.

Get Next 15 Events for league.
Get Last 15 Events for league.
Lookup Event Info.
Fetch Event Results by its ID.
Get Events for League during a particular Season.

Get Event Statistics by Id *Patreon ONLY*
Get Event Lineup by Id *Patreon ONLY*
Get List timeline for events by event ID *Patreon ONLY*


"""
from __future__ import absolute_import
import settings as TSD
from request import make_request


def eventLivescore(league_id: str):
    return make_request(TSD.EVENT_LIVESCORE, l=league_id)
    ...


def eventDay(day: str):
    return make_request(TSD.EVENTS_DAY, d=day)
    ...


def eventDayBySport(day: str, sport_id: str):
    return make_request(TSD.EVENTS_DAY, d=day, s=sport_id)
    ...


def eventDayByLeague(day: str, league_id: str):
    return make_request(TSD.EVENTS_DAY, d=day, l=league_id)
    ...


def eventTV(day: str):
    return make_request(TSD.EVENTS_TV, d=day)
    ...


def eventRound(league_id: str, round: str, season: str):
    return make_request(TSD.EVENTS_ROUND, id=league_id, r=round, s=season)


def eventTVByEvent(event_id: str):
    return make_request(TSD.LOOKUP_TV, id=event_id)


def eventTVBySport(day: str, sport_id: str):
    return make_request(TSD.EVENTS_TV, d=day, s=sport_id)
    ...


def eventStatistics(event_id: str):
    return make_request(TSD.EVENT_STATISTICS, id=event_id)
    ...

def eventLineup(event_id: str):
    return make_request(TSD.EVENT_LINEUP, id=event_id)
    ...

def eventTimeline(event_id: str):
    return make_request(TSD.EVENT_TIMELINE, id=event_id)
    ...


def nextLeagueEvents(league_id: str):
    return make_request(TSD.LEAGUE_NEXT_EVENTS, id=league_id)
    ...


def lastLeagueEvents(league_id: str):
    return make_request(TSD.LEAGUE_LAST_EVENTS, id=league_id)
    ...


def leagueSeasonEvents(league_id: str, season: str):
    return make_request(TSD.LEAGUE_SEASON_EVENTS, id=league_id, s=season)
    ...


def eventResult(event_id: str):
    return make_request(TSD.EVENT_RESULT, id=event_id)
    ...


def eventInfo(event_id: str):
    return make_request(TSD.EVENT, id=event_id)
    ...


def eventTimeline(event_id: str):
    return make_request(TSD.EVENT_TIMELINE, id=event_id)
    ...


def nextTeamEvents(team_id: str):
    return make_request(TSD.TEAM_NEXT_EVENTS, id=team_id)


def lastTeamEvents(team_id: str):
    return make_request(TSD.TEAM_LAST_EVENTS, id=team_id)


def searchEventsTVT(tvt_name: str):
    return make_request(TSD.SEARCH_EVENTS, e=tvt_name)


def searchEventsTVT(tvt_name: str, season: str):
    return make_request(TSD.SEARCH_EVENTS, e=tvt_name, s=season)
