import sqlite3
from sqlite3 import Error
import json
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

db_config_file = open('db_config.json')
db_config_data = json.load(db_config_file)

def create_sqlite_connection():
	conn = None
	try:
		conn = sqlite3.connect(db_config_data["db"]["sqlite"]["filename"])
		return conn
	except Error as e:
		print(e)
	
	return conn

def create_postgres_connection():
	conn = None
	try:
		conn = psycopg2.connect(user=db_config_data["db"]["postgresql"]["user"], password=db_config_data["db"]["postgresql"]["password"],host=db_config_data["db"]["postgresql"]["server"], port= db_config_data["db"]["postgresql"]["port"], database=db_config_data["db"]["postgresql"]["dbname"]);
		conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT);

		return conn
	except Error as e:
		print(e)
	
	return conn

def execute_sql_query(conn, sql_query):
	c = None
	try:
		c = conn.cursor()
		c.execute(sql_query)
	except Error as e:
		print(e)
	
	return c


def create_table_query(tablename,columns):
	query = f"CREATE TABLE IF NOT EXISTS {tablename} ("
	for idx,column in enumerate(columns):
		query += f"{column} TEXT"
		if ((idx+1) != len(columns)):
			query += ", "
	query += ")"

	return query

def create_select_query(tablename,columns):
	query = f"SELECT "
	if (len(columns) == 0):
		query += "*"
	else:
		for idx,column in enumerate(columns):
			query += f"{column} "
			if ((idx+1) != len(columns)):
				query += ", "
	query += f" FROM {tablename} ;"

	return query

def get_connection():
	conn = None
	try:
		if (db_config_data["active"] == "postgresql"):
			conn = create_postgres_connection()
		elif (db_config_data["active"] == "sqlite"):
			conn = create_sqlite_connection()
		return conn
	except Error as e:
		print(e)
	return conn

def get_dbtype():
	conn_type = None
	try:
		if (db_config_data["active"] == "postgresql"):
			conn = "pgsql"
		elif (db_config_data["active"] == "sqlite"):
			conn = "sqlite"
		return conn
	except Error as e:
		print(e)
	return conn

def flatten(l):
    return [item for sublist in l for item in sublist]