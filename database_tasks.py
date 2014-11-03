#! /usr/bin/python
import psycopg2

def create_connection(localhost,database,username,password):
  conn_string = "host='" + localhost + "' dbname='" + database + "' user='" + username + "' password='" + password + "'"
  conn = psycopg2.connect(conn_string)
  print("Connecting to database...")
  return(conn)

def create_staging_schema(cursor,schema_name):
  create_schema = "CREATE SCHEMA IF NOT EXISTS staging_" + schema_name + ";"
  #print(create_schema)
  cursor.execute(create_schema)

def create_staging_table(cursor,schema_name,table_name,column_names):
  create_table = "CREATE TABLE IF NOT EXISTS staging_" + schema_name + "." + table_name + " (" + " varchar(100),".join(column_names) + " varchar(100));"
  #print(create_table)
  cursor.execute(create_table.replace(",TO ",",TURNOVERS "))

def insert_staging_records(cursor,schema_name,table_name,column_names,records):
  for record in records:
    insert_record = "INSERT INTO staging_" + schema_name + "." + table_name + " (" + ",".join(column_names) + ") VALUES" + " ('" + "','".join(str(x).replace("'","''") for x in record) + "');"
    #print(insert_record)
    cursor.execute(insert_record.replace(",TO,",",TURNOVERS,"))
  print("Inserted " + str(len(records)) + " records into " + schema_name + "." + table_name)