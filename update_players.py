#! /usr/bin/python
import sys
import simplejson as json
import urllib
import database_tasks as db
import fetch_urls as fetch
import load_staging as load
import ConfigParser

if __name__ == "__main__":
  config = ConfigParser.ConfigParser()
  config.read("config.ini")
  localhost = config.get('postgresql','localhost')
  database = config.get('postgresql','database')
  username = config.get('postgresql','username')
  password = config.get('postgresql','password')
  
  conn = db.create_connection(localhost,database,username,password)

  try:
    season = sys.argv[1]
  except IndexError:
    season = '2014-15'

  cursor = conn.cursor()
  query = "SELECT DISTINCT table_schema FROM information_schema.tables WHERE table_schema LIKE 'staging_common%' OR table_schema LIKE 'staging_player%'";
  cursor.execute(query)
  rows = cursor.fetchall()
  for row in rows:
    drop_schema = "DROP SCHEMA " + row[0] + " CASCADE;"
    print(drop_schema)
    cursor.execute(drop_schema)
  conn.commit()
  
  players_url = "http://stats.nba.com/stats/commonallplayers?Season=" + season + "&LeagueID=00&IsOnlyCurrentSeason=0"
  players_data = json.loads(urllib.urlopen(players_url).read())
  print('\n' + "URL: " + players_url + '\n')

  for i in range(0,len(players_data["resultSets"])):
    table_schema = "staging_" + players_data["resource"].lower()
    table_name = players_data["resultSets"][i]["name"].lower()
    column_names = players_data["resultSets"][i]["headers"]
    records = players_data["resultSets"][i]["rowSet"]

    db.create_schema(cursor,table_schema)
    db.create_table(cursor,table_schema,table_name,column_names)
    db.insert_records(cursor,table_schema,table_name,column_names,records)
  
  player_ids = []
  if players_data["resultSets"][i]["name"] == 'CommonAllPlayers':
    for player in players_data["resultSets"][i]["rowSet"]:
      if player[4] in ('2014'):
        player_ids.append(player[0])
  print('Players found: ' + str(len(player_ids)))

  resources = []
  playerinfo_params = "?SeasonType=Regular+Season&LeagueID=00&PlayerID="
  playerlog_params = "?Season=" + season + "&SeasonType=Regular+Season&DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&Period=0&SeasonSegment=&TeamID=0&VsConference=&VsDivision=&PlayerID="

  resources.append(zip(['commonplayerinfo'],[playerinfo_params]))
  resources.append(zip(['playerdashptshotlog'],[playerlog_params]))
  resources.append(zip(['playerdashptreboundlogs'],[playerlog_params]))

  for resource in resources:
    print('\n' + "Loading resource: " + resource[0][0])
    urls = fetch.fetch_urls(player_ids,resource[0][0],resource[0][1])
    load.load_staging_tables(conn,urls,'player_id')

  conn.close()