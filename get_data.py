import requests
import sqlite3

from itms_urls import *

conn = sqlite3.connect('itms.db')

r = requests.get(base_itms_url+projekty_ukoncene)
print(r.json()[0].keys())
#print(r.json())


