import sqlite3
from sqlite3 import Error

def create_sqlite_connection(db_file):
	conn = None
	try:
		conn = sqlite3.connect(db_file)
		return conn
	except Error as e:
		print(e)
	
	return conn

def execute_sqlite_query(conn, sql_query):
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