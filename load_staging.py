#! /usr/bin/python
import sys
import simplejson as json
import urllib
from datetime import datetime, timedelta
import database_tasks as db
import fetch_urls as fetch
import ConfigParser

def load_staging_tables(conn,urls,id_name):
  game_ids = []
  cursor = conn.cursor()
  for url in urls:
    if fetch.validate_url(url):
      data = json.loads(urllib.urlopen(url).read())
      print('\n' + "URL: " + url + '\n')

      for i in range(0,len(data["resultSets"])):
        table_schema = "staging_" + data["resource"].lower()
        table_name = data["resultSets"][i]["name"].lower()
        column_names = data["resultSets"][i]["headers"]
        records = data["resultSets"][i]["rowSet"]
        column_name = id_name
        id_value = str(url.split('=')[-1])

        if id_name == 'game_date_est':
          id_value = datetime.strptime(id_value.replace('%2F','-'), "%m-%d-%Y")
          id_value = datetime.strftime(id_value, "%Y-%m-%dT%H:%M:%S")

        db.create_schema(cursor,table_schema)
        db.create_table(cursor,table_schema,table_name,column_names)
        db.insert_records(cursor,table_schema,table_name,column_names,records)
        
        if table_name == 'gameheader':
          for game in data["resultSets"][i]["rowSet"]:
            game_ids.append(game[2])
                    
        db.add_column_to_staging_table(cursor,table_schema,table_name,column_name)
        db.update_records(cursor,table_schema,table_name,column_name,id_value)
        conn.commit()

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

  conn = db.create_connection(localhost,database,username,password)
  
  try:
    start_date = datetime.strptime(sys.argv[1], "%Y-%m-%d")
    end_date = datetime.strptime(sys.argv[2], "%Y-%m-%d")
    dates = [start_date + timedelta(days=x) for x in range((end_date-start_date).days+1)]
  except IndexError:
    start_date = datetime.strptime('2014-10-28', "%Y-%m-%d") # 2014 Opening Night
    end_date = datetime.now()-timedelta(days=1)
    dates = [start_date + timedelta(days=x) for x in range((end_date-start_date).days+1)]
    
    cursor = conn.cursor()
    query = "SELECT DISTINCT game_date_est FROM staging_scoreboardv2.gameheader ORDER BY game_date_est;"
    cursor.execute(query)
    rows = cursor.fetchall()
    for row in rows:
      date = datetime.strptime(row[0], "%Y-%m-%dT%H:%M:%S")
      if date in dates: dates.remove(date)
    cursor.close()

  scoreboard_urls = fetch.fetch_scoreboard_urls(dates)
  if scoreboard_urls == None:
    print("Database looks up-to-date: No further games left to fetch!")
    sys.exit()
  game_ids = load_staging_tables(conn,scoreboard_urls,'game_date_est')
  print(game_ids)

  resources = []
  boxscore_resources = ['boxscoresummaryv2','boxscoreadvancedv2','boxscoremiscv2','boxscorescoringv2','boxscoreusagev2','boxscorefourfactorsv2','boxscoreplayertrackv2']
  boxscore_params = "?EndPeriod=0&EndRange=0&RangeType=0&Season=2014-15&SeasonType=Regular+Season&StartPeriod=0&StartRange=0&GameID="
  playbyplay_params = "?&EndPeriod=10&EndRange=55800&RangeType=2&Season=2014-15&SeasonType=Regular+Season&StartPeriod=1&StartRange=0&GameID="
  shotchart_params = "?&Season=2014-15&SeasonType=Regular+Season&LeagueID=00&TeamID=0&PlayerID=0&Outcome=&Location=&Month=0&SeasonSegment=&DateFrom=&DateTo=&OpponentTeamID=0&VsConference=&VsDivision=&Position=&RookieYear=&GameSegment=&Period=0&LastNGames=0&ContextFilter=&ContextMeasure=FG_PCT&display-mode=performance&zone-mode=zone&GameID="

  for boxscore_resource in boxscore_resources:
    resources.append(zip([boxscore_resource],[boxscore_params]))
  resources.append(zip(['playbyplayv2'],[playbyplay_params]))
  resources.append(zip(['shotchartdetail'],[shotchart_params]))

  for resource in resources:
    print('\n' + "Loading resource: " + resource[0][0])
    urls = fetch.fetch_urls(game_ids,resource[0][0],resource[0][1])
    load_staging_tables(conn,urls,'game_id')
   
  conn.close()