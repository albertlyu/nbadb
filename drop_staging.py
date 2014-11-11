#! /usr/bin/python
import database_tasks as db

if __name__ == "__main__":
  conn = db.create_connection()
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