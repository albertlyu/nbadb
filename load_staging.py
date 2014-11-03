#! /usr/bin/python
import sys
from datetime import datetime, timedelta
import urllib
import simplejson as json
import time
import psycopg2

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

def create_connection(localhost,database,username,password):
  conn_string = "host='" + localhost + "' dbname='" + database + "' user='" + username + "' password='" + password + "'"
  conn = psycopg2.connect(conn_string)
  print("Connecting to database...")
  return(conn)

def get_scoreboard_urls(start_date,end_date):
  d = start_date
  delta = timedelta(days=1)
  urls = []
  while d <= end_date:
    YYYY = str(d.year)
    MM = str(d.month)
    DD = str(d.day)
    scoreboard_url = NBA_BASE_URL + "scoreboardV2?DayOffset=0&LeagueID=00&gameDate=" + "%2F".join((MM,DD,YYYY)) 
    urls.append(scoreboard_url)
    d += delta
  return(urls)

def get_boxscore_urls(game_ids):
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

def get_playbyplay_urls(game_ids):
  urls = []
  for game_id in game_ids:
    playbyplay_url = NBA_BASE_URL + "playbyplayv2?GameID=" + game_id + "&EndPeriod=10&EndRange=55800&RangeType=2&Season=2014-15&SeasonType=Regular+Season&StartPeriod=1&StartRange=0" 
    shotchart_url = NBA_BASE_URL + "shotchartdetail?GameID=" + game_id + "&Season=2014-15&SeasonType=Regular+Season&LeagueID=00&TeamID=0&PlayerID=0&Outcome=&Location=&Month=0&SeasonSegment=&DateFrom=&DateTo=&OpponentTeamID=0&VsConference=&VsDivision=&Position=&RookieYear=&GameSegment=&Period=0&LastNGames=0&ContextFilter=&ContextMeasure=FG_PCT&display-mode=performance&zone-mode=zone"
    urls.append(playbyplay_url)
    urls.append(shotchart_url)
  return(urls)

def load_staging_tables(conn,urls):
  cursor = conn.cursor()
  game_ids = []
  for url in urls:
    if validate_url(url):
      urlRead = urllib.urlopen(url).read();
      data = json.loads(urlRead)
      print(url)
      for i in range(0,len(data["resultSets"])):
        schema_name = data["resource"]
        table_name = data["resultSets"][i]["name"]
        column_names = data["resultSets"][i]["headers"]
        records = data["resultSets"][i]["rowSet"]
        
        if table_name == 'GameHeader':
          for game in data["resultSets"][i]["rowSet"]:
            game_ids.append(game[2])

        create_schema = 'CREATE SCHEMA IF NOT EXISTS staging_' + schema_name + ';'
        print(create_schema)
        cursor.execute(create_schema)
        create_table = 'CREATE TABLE IF NOT EXISTS staging_' + schema_name + "." + table_name + ' (' + ' varchar(100),'.join(column_names) + ' varchar(100));'
        #print(create_table)
        cursor.execute(create_table.replace(",TO ",",TURNOVERS "))

        for record in records:
          insert_record = 'INSERT INTO staging_' + schema_name + "." + table_name + ' (' + ','.join(column_names) + ') VALUES' + " ('" + "','".join(str(x).replace("'","''") for x in record) + "');"
          #print(insert_record)
          cursor.execute(insert_record.replace(",TO,",",TURNOVERS,"))
        print("Inserted " + str(len(records)) + " records into " + table_name)
        
  conn.commit()
  cursor.close()
  return(game_ids)

if __name__ == "__main__":
  localhost = "localhost"
  database = "nbadb"
  username = "postgres"
  password = "postgres"
  
  try:
    start_date = datetime.strptime(sys.argv[1], "%Y-%m-%d")
    end_date = datetime.strptime(sys.argv[2], "%Y-%m-%d")
  except IndexError:
    print("I need a start and end date ('YYYY-MM-DD').")
    sys.exit()

  conn = create_connection(localhost,database,username,password)

  scoreboard_urls = get_scoreboard_urls(start_date,end_date)
  game_ids = load_staging_tables(conn,scoreboard_urls)

  boxscore_urls = get_boxscore_urls(game_ids)
  load_staging_tables(conn,boxscore_urls)

  playbyplay_urls = get_playbyplay_urls(game_ids)
  load_staging_tables(conn,playbyplay_urls)

  conn.close()
