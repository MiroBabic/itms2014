from tools import *
from itms_urls import *
import requests
import json

conn = create_sqlite_connection('itms.db')

itms_file = open('itms_urls.json')
itms_data = json.load(itms_file)
base_itms_url = itms_data["base_itms_url"]

for key,value in itms_data["data_objects_struct"].items():
	conn.row_factory = lambda cursor, row: row[0]
	get_url_query= f'SELECT {value["source_column"]} FROM {value["source_table"]}'
	urls = execute_sqlite_query(conn,get_url_query).fetchall()
	start = len(urls)
	for rec_url in urls:
		print(start)
		r = requests.get(base_itms_url+rec_url)
		columns = list(r.json().keys())
		columns_str = ",".join(columns)
		res_data = r.json()
		query = f"INSERT OR IGNORE INTO {key} ({columns_str}) VALUES ("
		for idx,column in enumerate(columns):
			if (column in res_data):
				insert_val = str(res_data[column]).replace("'","''")
				query += f"'{insert_val}'"
			else:
				query +="''"
			if (idx+1 != len(columns)):
					query += ", "
		query += ");"
		execute_sqlite_query(conn,query)
		start = start - 1