from tools import *
import requests
import json

#conn = create_sqlite_connection('itms.db')
conn = get_connection()

itms_file = open('itms_urls.json')
itms_data = json.load(itms_file)
base_itms_url = itms_data["base_itms_url"]
db_type = get_dbtype()

for key,value in itms_data["data_sources"].items():
	r = requests.get(base_itms_url+value["url"])
	columns = list(r.json()[0].keys())
	columns_str = ",".join(columns)
	res_data = r.json()
	start = len(res_data)
	#begin transaction
	execute_sql_query(conn,"BEGIN TRANSACTION;")
	print(key)
	for rec in res_data:
		print(start, end="\r")
		if db_type=="sqlite":
			query = f"INSERT OR IGNORE INTO {key} ({columns_str}) VALUES ("
		elif db_type=="pgsql":
			query = f"INSERT INTO {key} ({columns_str}) VALUES ("
		for idx,column in enumerate(columns):
			if (column in rec):
				insert_val = str(rec[column]).replace("'","''")
				query += f"'{insert_val}'"
			else:
				query +="''"
			if (idx+1 != len(columns)):
					query += ", "
		query += ")"
		#insert record
		if db_type =="pgsql":
			query+= f' ON CONFLICT ({value["id_field"]}) DO NOTHING'
		query += ";"
		execute_sql_query(conn,query)
		start = start -1
	#close transaction
	execute_sql_query(conn,"COMMIT;")	


