from tools import *
from itms_urls import *
import requests
import json
import numpy as np
import asyncio
import aiohttp
from asyncio_fix import *

async def get_resp(url, session):
    try:
        async with session.get(url=url) as response:
            resp = await response.json()
            return resp
    except Exception as e:
        print("Nepodarilo sa otvorit url {} . Dovod: {}.".format(url, e.__class__))

async def collect(chunk):
    async with aiohttp.ClientSession() as session:
        ret = await asyncio.gather(*[get_resp((base_itms_url+rec_url), session) for rec_url in chunk])
        return ret


conn = create_sqlite_connection('itms.db')

itms_file = open('itms_urls.json')
itms_data = json.load(itms_file)
base_itms_url = itms_data["base_itms_url"]


for key,value in itms_data["data_objects_struct"].items():
    print(key)
    conn.row_factory = lambda cursor, row: row[0]
    get_url_query= f'SELECT {value["source_column"]} FROM {value["source_table"]} WHERE {value["source_column"]} not in (select {value["source_column"]} from {key})'
    urls = execute_sqlite_query(conn,get_url_query).fetchall()
    start = len(urls)
    if (len(urls)==0):
        continue
    split_arr = np.array_split(urls,len(urls)/10)
    for chunk in split_arr:
        print(start)
        ret = asyncio.run(collect(chunk))
        print("mam ret")
        #print(ret)
        for record in ret:
            columns = list(record.keys())
            columns_str = ",".join(columns)
            res_data = record
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
            execute_sqlite_query(conn,"BEGIN TRANSACTION;")
            execute_sqlite_query(conn,query)
            execute_sqlite_query(conn,"COMMIT;")
            
        start = start - 10



##https://stackoverflow.com/questions/57126286/fastest-parallel-requests-in-python