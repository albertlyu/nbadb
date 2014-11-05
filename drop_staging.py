#! /usr/bin/python
import database_tasks as db
import ConfigParser

if __name__ == "__main__":
  config = ConfigParser.ConfigParser()
  config.read("config.ini")
  localhost = config.get('postgresql','localhost')
  database = config.get('postgresql','database')
  username = config.get('postgresql','username')
  password = config.get('postgresql','password')
  
  conn = db.create_connection(localhost,database,username,password)
  cursor = conn.cursor()
  query = "SELECT DISTINCT table_schema FROM information_schema.tables WHERE table_schema LIKE 'staging_%'";
  cursor.execute(query)
  rows = cursor.fetchall()
  for row in rows:
    drop_schema = "DROP SCHEMA " + row[0] + " CASCADE;"
    print(drop_schema)
    cursor.execute(drop_schema)

  conn.commit()
  cursor.close()
  conn.close()