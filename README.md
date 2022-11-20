# itms2014
python scraper for itms2014 data

Works with SQLite DB or PostgreSQL

DB Config:
Setup connectors and specify in "active" variable which DB you want to use
```
{	"active": "postgresql",
	"db": {
		"postgresql" :{
			"server" : "localhost",
			"port" :"5432",
			"dbname" :"itms",
			"user" : "postgres",
			"password" : "my_password"
		},
		"sqlite": {
		"filename": "itms.db"
		}
	}
}
```


itms_urls.json contains base tables to download (so far projekty a vyzvy) and detail structure for objects.
If you want to load data only for some table, set rest for active:false

Usage: 
```
python create_base_tables.py
```
```
python create_views.py
```
```
python get_base_data.py
```
```
python get_detail_data_multi.py  (if you want slow download one by one, use get_detail_data.py)
```

For exporting data into file use export.py - this works only for SQLite!

examples:

show available tables:
```
python export.py -f itms.db
```

simple export of one table:
```
python export.py -f itms.db -t projekt_ukonceny
```

export of one table with specified columns and delimiter (all data quoted)
```
python export.py -f itms.db -t projekty_total -c akronym aktivity cisloZmluvy datumKoncaRealizacie id intenzity kod meratelneUkazovatele partneri polozkyRozpoctu popisProjektu uzemneMechanizmy vyzva zameranieProjektu stav_implementacie miestaRealizacie_nuts3_gpsLat miestaRealizacie_nuts3_gpsLon -d ; -q
```

```
-f  file name with sqlite database
-t  table/view name to export
-c  column names separated by space (if no column provided, all columns will be exported)
-d  delimiter (if no delimiter specified, | is used as delimiter by default )
-q  quoted data (if not specified, only necessary records are delimited)
```
