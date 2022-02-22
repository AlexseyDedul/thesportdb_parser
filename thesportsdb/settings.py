BASE_URL = "https://www.thesportsdb.com/api/v1/json/"
BASE_URL_V2 = "https://www.thesportsdb.com/api/v2/json/"
API_KEY = "40130162"

# GET EVENTS
# ?id=133602 get next 5 events by teamId !!!!!!!!!!
TEAM_NEXT_EVENTS = "/eventsnext.php"
# ?id=133602 get last 5 events by teamid !!!!!!!!!!!!
TEAM_LAST_EVENTS = "/eventslast.php"
# ?id=4328 get next 15 events by leagueid !!!!!!!!!!!!!!!!!!
LEAGUE_NEXT_EVENTS = "/eventsnextleague.php"
# ?id=4328 get last 15 events by leagueid !!!!!!!!!!!!!!!
LEAGUE_LAST_EVENTS = "/eventspastleague.php"
# ?d=2014-10-10 ?d=2014-10-10&s=Soccer ?d=2014-10-10&l=4356 ?d=2014-10-10&l=Australian_A-League events on a specific day !!!!!!!!
EVENTS_DAY = "/eventsday.php"
# ?d=2018-07-07 ?d=2018-07-07&s=Fighting ?d=2019-09-28&a=United%20Kingdom&s=Cycling TV Events on a day (By Sport/Date/TV Station Country) !!!!!!!!!!
EVENTS_TV = "/eventstv.php"
EVENTS_ROUND = "/eventsround.php" # ?id=4328&r=38&s=2014-2015 Events in a specific round by league id/round/season !!!!!!
# ?id=4328&s=2014-2015 get league events for season using leagueid !!!!!!!!!!!!!!!
LEAGUE_SEASON_EVENTS = "/eventsseason.php"

# GET LOOKUPS
LEAGUE = "/lookupleague.php" # ?id=4346 League Details by leagueid !!!!!!!
TEAM = "/lookupteam.php"  # ?id=133604 lookup team by teamid !!!!!!!!!!
PLAYER = "/lookupplayer.php" # ?id=34145937 Player Details by Id !!!!!!!!!!

LEAGUE_TEAMS = "/lookup_all_teams.php"  # ?id=4328 get teams by leagueid !!!!!!!!!!!

# ?l=4328&s=2018-2019 get league table for season using league id !!!!!!!!!!!!!!!!!!
LEAGUE_SEASON_TABLE = "/lookuptable.php"


TEAM_PLAYERS = "/lookup_all_players.php" #?id=133604 get all players by teamId !!!!!!!!

# ?id=652890 lookup event results by eventid !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
EVENT_RESULT = "/eventresults.php"
EVENT = "/lookupevent.php"  # ?id=441613 lookup event by eventid !!!!!!!!!!!!!!!!!

EVENT_STATISTICS = "/lookupeventstats.php"  # ?id=1374558 get event statistics by id !!!!!!!!!!!!!!!!!!!!!!!!!!!!
EVENT_LINEUP = "/lookuplineup.php" # ?id=1374558 get event lineup by id !!!!!!!!!!!!!!!
EVENT_TIMELINE = "/lookuptimeline.php" # ?id=1374558 get list timeline by event id !!!!!!!!!
LOOKUP_HONORS = "/lookuphonors.php" # ?id=34147178 Player Honours by Player Id !!!!!!!!!!!
LOOKUP_FORMER_TEAMS = "/lookupformerteams.php" # ?id=34147178 Player Former Teams by Player Id !!!!!!!!!!!!!!!!!!!!!
LOOKUP_CONTRACTS = "/lookupcontracts.php" # ?id=34147178 Player Contracts by Player Id !!!!!!!!!!!!!!!!!!
LOOKUP_TV = "/lookuptv.php" # ?id=584911 Event TV by Event Id !!!!!!!!!!!!!!!!
LOOKUP_EQUIPMENT = "/lookupequipment.php" # ?id=133597 Lookup Equipment by Team ID !!!!!!!!!!

EVENT_LIVESCORE = "/livescore.php" # ?l=4328 get list livescore by league id !!!!!!!!!!!!!!!!!!!!!!

# SEARCH
ALL_SPORTS = "/all_sports.php"  # get all sports !!!!!!!
ALL_LEAGUES = "/all_leagues.php"  # get all leagues !!!!!!!!!!!!!!!!!!!
ALL_COUNTRIES = "/all_countries.php"  # get all countries !!!!!!!!!!!!!!!!
SEARCH_TEAMS = "/searchteams.php" # ?t=Arsenal Search for team by name
# ?p=Danny%20Welbeck Search for players by name !!!!!!!!!!!!!!!!!
SEARCH_PLAYERS = "/searchplayers.php"
SEARCH_EVENTS = "/searchevents.php" # ?e=Arsenal_vs_Chelsea ?e=Arsenal_vs_Chelsea&s=2016-2017 Search for event by event name !!!!!!!
SEARCH_EVENT_FILENAME = "/searchfilename.php" # ?e=English_Premier_League_2015-04-26_Arsenal_vs_Chelsea Search for event by event file name

# List all Seasons in a League ?id=4328
SEASONS = "/search_all_seasons.php"
