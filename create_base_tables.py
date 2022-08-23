from tools import *
import requests
import json

conn = create_sqlite_connection('itms.db')

itms_file = open('itms_urls.json')
itms_data = json.load(itms_file)
base_itms_url = itms_data["base_itms_url"]

for key,value in itms_data["data_sources"].items():
	r = requests.get(base_itms_url+value)
	columns = list(r.json()[0].keys())
	query = create_table_query(key,columns)
	execute_sqlite_query(conn,query)

for key,value in itms_data["data_objects_struct"].items():
	struct_file = open(value["source_file"])
	file_data = json.load(struct_file)
	columns = list(file_data.keys())
	query = create_table_query(key,columns)
	execute_sqlite_query(conn,query)

