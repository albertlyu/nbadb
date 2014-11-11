#! /usr/bin/python
import sys
import simplejson as json
import urllib
import database_tasks as db
import fetch_urls as fetch
import load_staging as load

if __name__ == "__main__":
  """
  Drop all existing player schemas and fetch player data and reload into db.
  """

  conn = db.create_connection()
  try:
    season = sys.argv[1]
  except IndexError:
    season = '2014-15'

  # Truncate all existing player tables (including player logs)
  cursor = conn.cursor()
  query = "SELECT DISTINCT table_schema, table_name FROM information_schema.tables WHERE table_schema LIKE 'staging_common%' OR table_schema LIKE 'staging_player%'";
  cursor.execute(query)
  rows = cursor.fetchall()
  for row in rows:
    table_schema = row[0]
    table_name = row[1]
    truncate_table = "TRUNCATE TABLE %s.%s;" % (table_schema,table_name)
    print(truncate_table)
    cursor.execute(truncate_table)
  conn.commit()
  
  # Load players data from season-level players URL
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
    
  # Store player IDs from players data
  player_ids = []
  if players_data["resultSets"][i]["name"] == 'CommonAllPlayers':
    for player in players_data["resultSets"][i]["rowSet"]:
      if player[4] in ('2014'): # Does not include players who retired before 2014
        player_ids.append(player[0])
  print('Players found: ' + str(len(player_ids)))

  # Initialize resources and parameter strings for URL concatenation
  resources = []
  playerinfo_params = "?SeasonType=Regular+Season&LeagueID=00&PlayerID="
  playerlog_params = "?Season=" + season + "&SeasonType=Regular+Season&DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&Period=0&SeasonSegment=&TeamID=0&VsConference=&VsDivision=&PlayerID="
  resources.append(zip(['commonplayerinfo'],[playerinfo_params]))
  resources.append(zip(['playerdashptshotlog'],[playerlog_params]))
  resources.append(zip(['playerdashptreboundlogs'],[playerlog_params]))

  # Load each resource's staging tables by player ID
  for resource in resources:
    print('\n' + "Loading resource: " + resource[0][0])
    urls = fetch.fetch_urls(player_ids,resource[0][0],resource[0][1])
    load.load_staging_tables(conn,urls,'player_id')

  conn.close()