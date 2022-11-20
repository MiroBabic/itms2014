from tools import *
import requests
import json


conn = get_connection()

itms_file = open('itms_urls.json')
itms_data = json.load(itms_file)
base_itms_url = itms_data["base_itms_url"]
db_type = get_dbtype()

for key,value in itms_data["data_sources"].items():
	r = requests.get(base_itms_url+value["url"])
	columns = list(r.json()[0].keys())
	query = create_table_query(key,columns)
	execute_sql_query(conn,query)
	if db_type=="pgsql":
		index_query= f'CREATE UNIQUE index {key}_uniq_id ON {key} ({value["id_field"]})'
		execute_sql_query(conn,index_query)

for key,value in itms_data["data_objects_struct"].items():
	struct_file = open(value["source_file"])
	file_data = json.load(struct_file)
	columns = list(file_data.keys())
	query = create_table_query(key,columns)
	execute_sql_query(conn,query)
	if db_type=="pgsql":
		index_query= f'CREATE UNIQUE index {key}_uniq_id ON {key} ({value["id_field"]})'
		execute_sql_query(conn,index_query)

