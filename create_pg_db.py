import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import json
from tools import *

db_config_file = open('db_config.json')
db_config_data = json.load(db_config_file)


conn = psycopg2.connect(user=db_config_data["db"]["postgresql"]["user"], password=db_config_data["db"]["postgresql"]["password"],host=db_config_data["db"]["postgresql"]["server"], port= db_config_data["db"]["postgresql"]["port"]);
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT);
cursor = conn.cursor();

sqlCreateDatabase = "create database "+db_config_data["db"]["postgresql"]["dbname"]+";"

cursor.execute(sqlCreateDatabase);