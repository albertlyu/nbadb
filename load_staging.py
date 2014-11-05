#! /usr/bin/python
import sys
import simplejson as json
import urllib
from datetime import datetime
import database_tasks as db
import fetch_urls as fetch
import ConfigParser

def load_staging_tables(conn,urls):
  cursor = conn.cursor()
  game_ids = []
  for url in urls:
    if fetch.validate_url(url):
      data = json.loads(urllib.urlopen(url).read())
      print(url)
      for i in range(0,len(data["resultSets"])):
        schema_name = data["resource"]
        table_name = data["resultSets"][i]["name"]
        column_names = data["resultSets"][i]["headers"]
        records = data["resultSets"][i]["rowSet"]
        
        db.create_staging_schema(cursor,schema_name)
        db.create_staging_table(cursor,schema_name,table_name,column_names)
        db.insert_staging_records(cursor,schema_name,table_name,column_names,records)
        
        if table_name == 'GameHeader':
          for game in data["resultSets"][i]["rowSet"]:
            game_ids.append(game[2])
                    
  conn.commit()
  cursor.close()
  return(game_ids)

if __name__ == "__main__":
  config = ConfigParser.ConfigParser()
  config.read("config.ini")
  localhost = config.get('postgresql','localhost')
  database = config.get('postgresql','database')
  username = config.get('postgresql','username')
  password = config.get('postgresql','password')

  try:
    start_date = datetime.strptime(sys.argv[1], "%Y-%m-%d")
    end_date = datetime.strptime(sys.argv[2], "%Y-%m-%d")
  except IndexError:
    print("I need a start and end date ('YYYY-MM-DD').")
    sys.exit()

  conn = db.create_connection(localhost,database,username,password)

  scoreboard_urls = fetch.fetch_scoreboard_urls(start_date,end_date)
  game_ids = load_staging_tables(conn,scoreboard_urls)

  boxscore_urls = fetch.fetch_boxscore_urls(game_ids)
  load_staging_tables(conn,boxscore_urls)

  playbyplay_urls = fetch.fetch_playbyplay_urls(game_ids)
  load_staging_tables(conn,playbyplay_urls)

  conn.close()