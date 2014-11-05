#! /usr/bin/python
import psycopg2

def create_connection(localhost,database,username,password):
  conn_string = "host='" + localhost + "' dbname='" + database + "' user='" + username + "' password='" + password + "'"
  print("Connecting to database...")
  conn = psycopg2.connect(conn_string)
  print("Connection established!")
  return(conn)

def create_schema(cursor,table_schema):
  create_schema = "CREATE SCHEMA IF NOT EXISTS " + table_schema + ";"
  #print(create_schema)
  cursor.execute(create_schema)
  print("Created " + table_schema + " schema")

def create_table(cursor,table_schema,table_name,column_names):
  create_table = "CREATE TABLE IF NOT EXISTS " + table_schema + "." + table_name + " (" + " varchar(100),".join(column_names) + " varchar(100));"
  #print(create_table)
  cursor.execute(create_table.replace(",TO ",",TURNOVERS "))
  print("Created " + table_schema + "." + table_name + " table")

def insert_records(cursor,table_schema,table_name,column_names,records):
  for record in records:
    insert_record = "INSERT INTO " + table_schema + "." + table_name + " (" + ",".join(column_names) + ") VALUES" + " ('" + "','".join(str(x).replace("'","''") for x in record) + "');"
    #print(insert_record)
    cursor.execute(insert_record.replace(",TO,",",TURNOVERS,"))
  print("Inserted " + str(len(records)) + " records into " + table_schema + "." + table_name)

def check_if_column_exists(cursor,table_schema,table_name,column_name):
  query = "SELECT * FROM information_schema.columns WHERE table_schema = '" + table_schema + "' AND table_name = '" + table_name + "' AND column_name = '" + column_name + "';"
  #print(query)
  cursor.execute(query)
  rows = cursor.fetchall()
  if len(rows) == 1:
    return(rows)

def add_column_to_staging_table(cursor,table_schema,table_name,column_name):
  if check_if_column_exists(cursor,table_schema,table_name,column_name):
    print("Column " + column_name + " already exists in " + table_schema + "." + table_name)
  else:
    add_column = "ALTER TABLE " + table_schema + "." + table_name + " ADD COLUMN " + column_name + " varchar(100);"
    #print(add_column)
    cursor.execute(add_column)
    print("Added " + column_name + " to " + table_schema + "." + table_name)

def update_records(cursor,table_schema,table_name,column_name,value):
  update_records = "UPDATE " + table_schema + "." + table_name + " SET " + column_name + "='" + value + "' WHERE COALESCE(" + column_name + ",'')='';"
  #print(update_records)
  cursor.execute(update_records)
  print("Updated records in " + table_schema + "." + table_name + " where " + column_name + " is empty")