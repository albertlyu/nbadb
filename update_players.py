#! /usr/bin/python
import simplejson as json
import urllib
import database_tasks as db
import fetch_urls as fetch
import load_staging as load

if __name__ == "__main__":
  localhost = "localhost"
  database = "nbadb"
  username = "postgres"
  password = "postgres"
  
  conn = db.create_connection(localhost,database,username,password)
  cursor = conn.cursor()

  players_url = "http://stats.nba.com/stats/commonallplayers?LeagueID=00&Season=2014-15&IsOnlyCurrentSeason=0"
  players_data = json.loads(urllib.urlopen(players_url).read())

  for i in range(0,len(players_data["resultSets"])):
    schema_name = players_data["resource"]
    table_name = players_data["resultSets"][i]["name"]
    column_names = players_data["resultSets"][i]["headers"]
    records = players_data["resultSets"][i]["rowSet"]

    db.create_staging_schema(cursor,schema_name)
    db.create_staging_table(cursor,schema_name,table_name,column_names)
    db.insert_staging_records(cursor,schema_name,table_name,column_names,records)
  
  player_ids = []
  if players_data["resultSets"][i]["name"] == 'CommonAllPlayers':
    for player in players_data["resultSets"][i]["rowSet"]:
      if player[4] == '2014':
        player_ids.append(player[0])

  print(player_ids)
  print(str(len(player_ids)))

  player_urls = fetch.fetch_player_urls(player_ids)
  load.load_staging_tables(conn,player_urls)

  conn.commit()
  cursor.close()
  conn.close()