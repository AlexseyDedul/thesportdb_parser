from countries import allCountries
from leagues import *
from events import *
from players import *
from sports import *
from teams import *
import asyncio


async def main():
    asyncio.create_task(leagueTeams("4328"))
    asyncio.create_task(teamInfo("133604"))
    asyncio.create_task(searchTeamsByName("Arsenal"))
    asyncio.create_task(searchTeamsByName("Arsenal"))
    asyncio.create_task(allCountries())

    # print(allCountries())

    # print(eventLivescore("4387")) v2

    # print(eventStatistics("1032723"))
    # print(eventLineup("1032723"))
    # print(eventTimeline("1032718"))
    # print(nextLeagueEvents("4328"))
    # print(lastLeagueEvents("4328"))
    # print(leagueSeasonEvents("4328", "2014-2015"))
    # print(eventResult("652890"))
    # print(eventInfo("441613"))

    # print(allLeagues())
    # print(leagueSeasonTable("4328", "2018-2019"))
    # print(leagueInfo("4328"))
    # print(sportLeagues("102")) ????????????????

    # print(teamPlayers("133604"))
    # print(playersHonours("34147178"))
    # print(playerDetails("34145937"))
    # print(searchPlayersByName("Danny Welbeck"))
    # print(playersFormerTeam("34147178"))
    # print(playersContracts("34147178"))

    # print(allSports())


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
