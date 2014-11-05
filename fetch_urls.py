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

def fetch_player_urls(player_ids):
  urls = []
  for player_id in player_ids:
    player_url = NBA_BASE_URL + "commonplayerinfo?PlayerID=" + str(player_id) + "&SeasonType=Regular+Season&LeagueID=00"
    urls.append(player_url)
  return(urls)

def fetch_scoreboard_urls(dates):
  urls = []
  if dates == []:
    print("Database looks up-to-date: No games left to fetch!")
    return
  for date in dates:
    YYYY = str(date.year)
    MM = str(date.month)
    DD = str(date.day)
    scoreboard_url = NBA_BASE_URL + "scoreboardV2?DayOffset=0&LeagueID=00&gameDate=" + "%2F".join((MM,DD,YYYY)) 
    urls.append(scoreboard_url)
  return(urls)

def fetch_boxscore_urls(game_ids):
  urls = []
  for game_id in game_ids:
    boxscore_params = "&EndPeriod=0&EndRange=0&RangeType=0&Season=2014-15&SeasonType=Regular+Season&StartPeriod=0&StartRange=0"
    
    #boxscore_url = NBA_BASE_URL + "boxscore?GameID=" + game_id + "&RangeType=0&StartPeriod=0&EndPeriod=0&StartRange=0&EndRange=0"
    boxscoresummary_url = NBA_BASE_URL + "boxscoresummaryv2?GameID=" + game_id
    #boxscoretraditional_url = NBA_BASE_URL + "boxscoretraditionalv2?GameID=" + game_id + boxscore_params
    boxscoreadvanced_url = NBA_BASE_URL + "boxscoreadvancedv2?GameID=" + game_id + boxscore_params
    boxscoremisc_url = NBA_BASE_URL + "boxscoremiscv2?GameID=" + game_id + boxscore_params
    boxscorescoring_url = NBA_BASE_URL + "boxscorescoringv2?GameID=" + game_id + boxscore_params
    boxscoreusage_url = NBA_BASE_URL + "boxscoreusagev2?GameID=" + game_id + boxscore_params
    boxscorefourfactors_url = NBA_BASE_URL + "boxscorefourfactorsv2?GameID=" + game_id + boxscore_params
    boxscoreplayertrack_url = NBA_BASE_URL + "boxscoreplayertrackv2?GameID=" + game_id + boxscore_params

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
  for game_id in game_ids:
    playbyplay_url = NBA_BASE_URL + "playbyplayv2?GameID=" + game_id + "&EndPeriod=10&EndRange=55800&RangeType=2&Season=2014-15&SeasonType=Regular+Season&StartPeriod=1&StartRange=0" 
    shotchart_url = NBA_BASE_URL + "shotchartdetail?GameID=" + game_id + "&Season=2014-15&SeasonType=Regular+Season&LeagueID=00&TeamID=0&PlayerID=0&Outcome=&Location=&Month=0&SeasonSegment=&DateFrom=&DateTo=&OpponentTeamID=0&VsConference=&VsDivision=&Position=&RookieYear=&GameSegment=&Period=0&LastNGames=0&ContextFilter=&ContextMeasure=FG_PCT&display-mode=performance&zone-mode=zone"
    urls.append(playbyplay_url)
    urls.append(shotchart_url)
  return(urls)