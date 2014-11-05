#! /usr/bin/python
import psycopg2

def create_connection(localhost,database,username,password):
  conn_string = "host='" + localhost + "' dbname='" + database + "' user='" + username + "' password='" + password + "'"
  conn = psycopg2.connect(conn_string)
  print("Connecting to database...")
  return(conn)

def create_schema(cursor,table_schema):
  create_schema = "CREATE SCHEMA IF NOT EXISTS " + table_schema + ";"
  #print(create_schema)
  cursor.execute(create_schema)

def create_table(cursor,table_schema,table_name,column_names):
  create_table = "CREATE TABLE IF NOT EXISTS " + table_schema + "." + table_name + " (" + " varchar(100),".join(column_names) + " varchar(100));"
  #print(create_table)
  cursor.execute(create_table.replace(",TO ",",TURNOVERS "))

def insert_records(cursor,table_schema,table_name,column_names,records):
  for record in records:
    insert_record = "INSERT INTO " + table_schema + "." + table_name + " (" + ",".join(column_names) + ") VALUES" + " ('" + "','".join(str(x).replace("'","''") for x in record) + "');"
    #print(insert_record)
    cursor.execute(insert_record.replace(",TO,",",TURNOVERS,"))
  print("Inserted " + str(len(records)) + " records into " + table_schema + "." + table_name)