from tools import *
import requests
import json

conn = get_connection()

itms_file = open('itms_urls.json')
itms_data = json.load(itms_file)
base_itms_url = itms_data["base_itms_url"]
db_type = get_dbtype()

for key,value in itms_data["data_objects_struct"].items():
	if db_type =="sqlite":
		conn.row_factory = lambda cursor, row: row[0]
	get_url_query= f'SELECT {value["source_column"]} FROM {value["source_table"]} WHERE {value["source_column"]} not in (select {value["source_column"]} from {key})'
	urls = execute_sql_query(conn,get_url_query).fetchall()
	#print(urls)
	start = len(urls)
	for rec_url in urls:
		print(start)
		if db_type =="pgsql":
			rec_url = rec_url[0]
		r = requests.get(base_itms_url+rec_url)
		columns = list(r.json().keys())
		columns_str = ",".join(columns)
		res_data = r.json()
		if db_type=="sqlite":
			query = f"INSERT OR IGNORE INTO {key} ({columns_str}) VALUES ("
		elif db_type=="pgsql":
			query = f"INSERT INTO {key} ({columns_str}) VALUES ("
		
		for idx,column in enumerate(columns):
			if (column in res_data):
				insert_val = str(res_data[column]).replace("'","''")
				query += f"'{insert_val}'"
			else:
				query +="''"
			if (idx+1 != len(columns)):
					query += ", "
		query += ")"
		if db_type =="pgsql":
			query+= f' ON CONFLICT ({value["id_field"]}) DO NOTHING'
		query += ";"
		execute_sql_query(conn,"BEGIN TRANSACTION;")
		execute_sql_query(conn,query)
		execute_sql_query(conn,"COMMIT;")
		start = start - 1