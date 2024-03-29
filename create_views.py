from tools import *
import requests
import json

#conn = create_sqlite_connection('itms.db')
conn = get_connection()
db_type = get_dbtype()

#query = """CREATE VIEW IF NOT EXISTS projekty_total AS
query_sqlite = """CREATE OR REPLACE VIEW  projekty_total AS
select pu.*, "ukoncenie" as stav_implementacie,
json_extract(json_extract(jsn.value,'$.nuts3'),'$.gpsLat') as miestaRealizacie_nuts3_gpsLat,
json_extract(json_extract(jsn.value,'$.nuts3'),'$.gpsLon') as miestaRealizacie_nuts3_gpsLon,
json_extract(json_extract(jsn.value,'$.nuts4'),'$.gpsLat') as miestaRealizacie_nuts4_gpsLat,
json_extract(json_extract(jsn.value,'$.nuts4'),'$.gpsLon') as miestaRealizacie_nuts4_gpsLon,
json_extract(json_extract(jsn.value,'$.nuts5'),'$.gpsLat') as miestaRealizacie_nuts5_gpsLat,
json_extract(json_extract(jsn.value,'$.nuts5'),'$.gpsLon') as miestaRealizacie_nuts5_gpsLon,
json_extract(json_extract(jsn_mimo.value,'$.nuts3'),'$.gpsLat') as miestaRealizacieMimoUzemiaOP_nuts3_gpsLat,
json_extract(json_extract(jsn_mimo.value,'$.nuts3'),'$.gpsLon') as miestaRealizacieMimoUzemiaOP_nuts3_gpsLon,
json_extract(json_extract(jsn_mimo.value,'$.nuts4'),'$.gpsLat') as miestaRealizacieMimoUzemiaOP_nuts4_gpsLat,
json_extract(json_extract(jsn_mimo.value,'$.nuts4'),'$.gpsLon') as miestaRealizacieMimoUzemiaOP_nuts4_gpsLon,
json_extract(json_extract(jsn_mimo.value,'$.nuts5'),'$.gpsLat') as miestaRealizacieMimoUzemiaOP_nuts5_gpsLat,
json_extract(json_extract(jsn_mimo.value,'$.nuts5'),'$.gpsLon') as miestaRealizacieMimoUzemiaOP_nuts5_gpsLon
from projekt_ukonceny pu 
left outer join json_each(replace(pu.miestaRealizacie,"'",'"'), "$") as jsn
left outer join json_each(replace(pu.miestaRealizacieMimoUzemiaOP,"'",'"'), "$") as jsn_mimo
union all
select pv.*, "realizacia" as stav_implementacie,
json_extract(json_extract(jsn.value,'$.nuts3'),'$.gpsLat') as miestaRealizacie_nuts3_gpsLat,
json_extract(json_extract(jsn.value,'$.nuts3'),'$.gpsLon') as miestaRealizacie_nuts3_gpsLon,
json_extract(json_extract(jsn.value,'$.nuts4'),'$.gpsLat') as miestaRealizacie_nuts4_gpsLat,
json_extract(json_extract(jsn.value,'$.nuts4'),'$.gpsLon') as miestaRealizacie_nuts4_gpsLon,
json_extract(json_extract(jsn.value,'$.nuts5'),'$.gpsLat') as miestaRealizacie_nuts5_gpsLat,
json_extract(json_extract(jsn.value,'$.nuts5'),'$.gpsLon') as miestaRealizacie_nuts5_gpsLon,
json_extract(json_extract(jsn_mimo.value,'$.nuts3'),'$.gpsLat') as miestaRealizacieMimoUzemiaOP_nuts3_gpsLat,
json_extract(json_extract(jsn_mimo.value,'$.nuts3'),'$.gpsLon') as miestaRealizacieMimoUzemiaOP_nuts3_gpsLon,
json_extract(json_extract(jsn_mimo.value,'$.nuts4'),'$.gpsLat') as miestaRealizacieMimoUzemiaOP_nuts4_gpsLat,
json_extract(json_extract(jsn_mimo.value,'$.nuts4'),'$.gpsLon') as miestaRealizacieMimoUzemiaOP_nuts4_gpsLon,
json_extract(json_extract(jsn_mimo.value,'$.nuts5'),'$.gpsLat') as miestaRealizacieMimoUzemiaOP_nuts5_gpsLat,
json_extract(json_extract(jsn_mimo.value,'$.nuts5'),'$.gpsLon') as miestaRealizacieMimoUzemiaOP_nuts5_gpsLon
from projekt_vrealizacii pv
left outer join json_each(replace(pv.miestaRealizacie,"'",'"'), "$") as jsn
left outer join json_each(replace(pv.miestaRealizacieMimoUzemiaOP,"'",'"'), "$") as jsn_mimo;"""

query_postgre = """DROP VIEW IF EXISTS projekty_total; CREATE VIEW projekty_total AS
select pu.*, 'ukoncenie' as stav_implementacie,
json_array_elements((replace(miestarealizacie,'''','"')::json))->'nuts3'->'gpsLat' as miestaRealizacie_nuts3_gpsLat,
json_array_elements((replace(miestarealizacie,'''','"')::json))->'nuts3'->'gpsLon' as miestaRealizacie_nuts3_gpsLon,
json_array_elements((replace(miestarealizacie,'''','"')::json))->'nuts4'->'gpsLat' as miestaRealizacie_nuts4_gpsLat,
json_array_elements((replace(miestarealizacie,'''','"')::json))->'nuts4'->'gpsLon' as miestaRealizacie_nuts4_gpsLon,
json_array_elements((replace(miestarealizacie,'''','"')::json))->'nuts5'->'gpsLat' as miestaRealizacie_nuts5_gpsLat,
json_array_elements((replace(miestarealizacie,'''','"')::json))->'nuts5'->'gpsLon' as miestaRealizacie_nuts5_gpsLon,
json_array_elements((replace(miestaRealizacieMimoUzemiaOP,'''','"')::json))->'nuts3'->'gpsLat' as miestaRealizacieMimoUzemiaOP_nuts3_gpsLat,
json_array_elements((replace(miestaRealizacieMimoUzemiaOP,'''','"')::json))->'nuts3'->'gpsLon' as miestaRealizacieMimoUzemiaOP_nuts3_gpsLon,
json_array_elements((replace(miestaRealizacieMimoUzemiaOP,'''','"')::json))->'nuts4'->'gpsLat' as miestaRealizacieMimoUzemiaOP_nuts4_gpsLat,
json_array_elements((replace(miestaRealizacieMimoUzemiaOP,'''','"')::json))->'nuts4'->'gpsLon' as miestaRealizacieMimoUzemiaOP_nuts4_gpsLon,
json_array_elements((replace(miestaRealizacieMimoUzemiaOP,'''','"')::json))->'nuts5'->'gpsLat' as miestaRealizacieMimoUzemiaOP_nuts5_gpsLat,
json_array_elements((replace(miestaRealizacieMimoUzemiaOP,'''','"')::json))->'nuts5'->'gpsLon' as miestaRealizacieMimoUzemiaOP_nuts5_gpsLon
FROM public.projekt_ukonceny pu
union all
select pv.*, 'realizacia' as stav_implementacie,
json_array_elements((replace(miestarealizacie,'''','"')::json))->'nuts3'->'gpsLat' as miestaRealizacie_nuts3_gpsLat,
json_array_elements((replace(miestarealizacie,'''','"')::json))->'nuts3'->'gpsLon' as miestaRealizacie_nuts3_gpsLon,
json_array_elements((replace(miestarealizacie,'''','"')::json))->'nuts4'->'gpsLat' as miestaRealizacie_nuts4_gpsLat,
json_array_elements((replace(miestarealizacie,'''','"')::json))->'nuts4'->'gpsLon' as miestaRealizacie_nuts4_gpsLon,
json_array_elements((replace(miestarealizacie,'''','"')::json))->'nuts5'->'gpsLat' as miestaRealizacie_nuts5_gpsLat,
json_array_elements((replace(miestarealizacie,'''','"')::json))->'nuts5'->'gpsLon' as miestaRealizacie_nuts5_gpsLon,
json_array_elements((replace(miestaRealizacieMimoUzemiaOP,'''','"')::json))->'nuts3'->'gpsLat' as miestaRealizacieMimoUzemiaOP_nuts3_gpsLat,
json_array_elements((replace(miestaRealizacieMimoUzemiaOP,'''','"')::json))->'nuts3'->'gpsLon' as miestaRealizacieMimoUzemiaOP_nuts3_gpsLon,
json_array_elements((replace(miestaRealizacieMimoUzemiaOP,'''','"')::json))->'nuts4'->'gpsLat' as miestaRealizacieMimoUzemiaOP_nuts4_gpsLat,
json_array_elements((replace(miestaRealizacieMimoUzemiaOP,'''','"')::json))->'nuts4'->'gpsLon' as miestaRealizacieMimoUzemiaOP_nuts4_gpsLon,
json_array_elements((replace(miestaRealizacieMimoUzemiaOP,'''','"')::json))->'nuts5'->'gpsLat' as miestaRealizacieMimoUzemiaOP_nuts5_gpsLat,
json_array_elements((replace(miestaRealizacieMimoUzemiaOP,'''','"')::json))->'nuts5'->'gpsLon' as miestaRealizacieMimoUzemiaOP_nuts5_gpsLon
FROM public.projekt_vrealizacii pv;"""

if db_type=="sqlite":
	execute_sql_query(conn,query_sqlite)
elif db_type=="pgsql":
	execute_sql_query(conn,query_postgre)
