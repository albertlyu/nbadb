#! /usr/bin/python
import urllib

NBA_BASE_URL = "http://stats.nba.com/stats/"

def validate_url(url):
  try:
    urllib.urlopen(url)
  except urllib.error.HTTPError as e:
    print("Error code:", e.code)
    print(url)
  except urllib.error.URLError as e:
    print("We failed to reach a server. Reason: ", e.reason)
  else:
    return True

def fetch_scoreboard_urls(dates):
  urls = []
  scoreboard_params = "scoreboardV2?DayOffset=0&LeagueID=00&gameDate="
  if dates == []:
    print("Database looks up-to-date: No games left to fetch!")
    return
  for date in dates:
    YYYY = str(date.year)
    MM = str(date.month)
    DD = str(date.day)
    scoreboard_url = NBA_BASE_URL + scoreboard_params + "%2F".join((MM,DD,YYYY)) 
    urls.append(scoreboard_url)
  return(urls)

def fetch_boxscore_urls(game_ids):
  urls = []
  boxscore_params = "EndPeriod=0&EndRange=0&RangeType=0&Season=2014-15&SeasonType=Regular+Season&StartPeriod=0&StartRange=0&GameID="
  for game_id in game_ids:  
    #boxscore_url = NBA_BASE_URL + "boxscore?GameID=" + game_id + "&RangeType=0&StartPeriod=0&EndPeriod=0&StartRange=0&EndRange=0"
    boxscoresummary_url = NBA_BASE_URL + "boxscoresummaryv2?GameID=" + game_id
    #boxscoretraditional_url = NBA_BASE_URL + "boxscoretraditionalv2?" + boxscore_params + game_id 
    boxscoreadvanced_url = NBA_BASE_URL + "boxscoreadvancedv2?" + boxscore_params + game_id
    boxscoremisc_url = NBA_BASE_URL + "boxscoremiscv2?" + boxscore_params + game_id
    boxscorescoring_url = NBA_BASE_URL + "boxscorescoringv2?" + boxscore_params + game_id
    boxscoreusage_url = NBA_BASE_URL + "boxscoreusagev2?" + boxscore_params + game_id
    boxscorefourfactors_url = NBA_BASE_URL + "boxscorefourfactorsv2?" + boxscore_params + game_id
    boxscoreplayertrack_url = NBA_BASE_URL + "boxscoreplayertrackv2?" + boxscore_params + game_id
    #urls.append(boxscore_url)
    urls.append(boxscoresummary_url)
    #urls.append(boxscoretraditional_url)
    urls.append(boxscoreadvanced_url)
    urls.append(boxscoremisc_url)
    urls.append(boxscorescoring_url)
    urls.append(boxscoreusage_url)
    urls.append(boxscorefourfactors_url)
    urls.append(boxscoreplayertrack_url)
  return(urls)

def fetch_playbyplay_urls(game_ids):
  urls = []
  playbyplay_params = "playbyplayv2?&EndPeriod=10&EndRange=55800&RangeType=2&Season=2014-15&SeasonType=Regular+Season&StartPeriod=1&StartRange=0&GameID="
  shotchart_params = "shotchartdetail?&Season=2014-15&SeasonType=Regular+Season&LeagueID=00&TeamID=0&PlayerID=0&Outcome=&Location=&Month=0&SeasonSegment=&DateFrom=&DateTo=&OpponentTeamID=0&VsConference=&VsDivision=&Position=&RookieYear=&GameSegment=&Period=0&LastNGames=0&ContextFilter=&ContextMeasure=FG_PCT&display-mode=performance&zone-mode=zone&GameID="
  for game_id in game_ids:
    playbyplay_url = NBA_BASE_URL + playbyplay_params + game_id
    shotchart_url = NBA_BASE_URL + shotchart_params + game_id
    urls.append(playbyplay_url)
    urls.append(shotchart_url)
  return(urls)

def fetch_player_urls(player_ids):
  urls = []
  player_params = "commonplayerinfo?SeasonType=Regular+Season&LeagueID=00&PlayerID="
  for player_id in player_ids:
    player_url = NBA_BASE_URL + player_params + str(player_id)
    urls.append(player_url)
  return(urls)

def fetch_playerlog_urls(player_ids):
  urls = []
  shot_params = "playerdashptshotlog?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&Period=0&Season=2014-15&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision=&PlayerID="
  reb_params = "playerdashptreboundlogs?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&Period=0&Season=2014-15&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision=&PlayerID="
  for player_id in player_ids:
    playershotlog_url = NBA_BASE_URL + shot_params + str(player_id)
    playerreblog_url = NBA_BASE_URL + reb_params + str(player_id)
    urls.append(playershotlog_url)
    urls.append(playerreblog_url)
  return(urls)