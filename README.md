# itms2014
python scraper for itms2014 data

As db is used SQLite, connector to any real db can be modified in tools.py.

itms_urls.json contains base tables to download (so far projekty a vyzvy) and detail structure for objects.

Usage: 

python create_base_tables.py

python get_base_data.py

python get_detail_data_multi.py  (if you want slow download one by one, use get_detail_data.py)


