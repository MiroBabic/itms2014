import requests
from itms_urls import *

r = requests.get(base_itms_url+vyzvy_planovane)
print(r.json()[0].keys())


