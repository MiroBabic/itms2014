###
### imports
###

import argparse
import csv
from tools import *
import pandas as pd
import sqlite3
from sqlite3 import Error
from contextlib import closing

###
### functions
###

def export_table(dbfile,table_to_export,columns,export_delimiter,export_quoting):
	conn = create_sqlite_connection(dbfile)
	print(f"Exporting...")
	query = create_select_query(table_to_export,columns)
	exported_data = execute_sqlite_query(conn,query)
	num_fields = len(exported_data.description)
	field_names = [i[0] for i in exported_data.description]
	
	out_file= open('export.csv', 'w',encoding='utf-8', newline='')
	f = csv.writer(out_file, delimiter=export_delimiter, quotechar='"', quoting=export_quoting)
	f.writerow(field_names)
	f.writerows(exported_data.fetchall())
	out_file.close()

def show_tables(dbfile):
	conn = create_sqlite_connection(dbfile)
	tables_available = execute_sqlite_query(conn,"""SELECT name FROM sqlite_master;""")
	print(f"Following tables and columns are available:")
	for table in tables_available.fetchall():
		available_table = str(table[0])
		print(available_table)

###
### initialize argparse parser
###

hello_msg = "Export.py is used for exporting data from a SQLite database to CSV file."
parser = argparse.ArgumentParser(description = hello_msg)

###
### arguments
###

parser.add_argument("-f", "--dbfile", help = "use SQLite database file (required option)", action = "store", dest = "dbfile", required = True)
parser.add_argument("-t", "--table", help = "export a table", action = "store", dest = "table")
parser.add_argument("-c", "--columns", help = "export particular columns (if no column provided, all will be exported)", nargs="*", dest = "columns")
parser.add_argument("-d", "--delimiter", help = "use different CSV delimiter (default is |)", action = "store", dest = "delimiter")
parser.add_argument("-q", "--quote", help = "quote all exported data (default is to quote only where needed)", action = "store_true")

args = parser.parse_args()

###
### main
###

if args.dbfile and not args.table:
	dbfile = args.dbfile
	show_tables(dbfile)

if args.dbfile and args.table:
	dbfile = args.dbfile
	table_to_export = args.table

	if args.delimiter:
		export_delimiter = args.delimiter
	else:
		export_delimiter = "|"

	if args.columns:
		export_columns = args.columns
	else:
		export_columns = []

	if args.quote is False:
		export_quoting = csv.QUOTE_MINIMAL
		quoting_info = "no, quoting data only where needed"
	else:
		export_quoting = csv.QUOTE_ALL
		quoting_info = "yes, quoting all exported data"

	print(f"SQLite database file: {dbfile}")
	print(f"Exported table: {table_to_export}")
	print(f"Delimiter for exported data: {export_delimiter}")
	print(f"Quoting of exported data: {quoting_info}")

	export_table(dbfile,table_to_export,export_columns,export_delimiter, export_quoting)

print("----Done----.")
