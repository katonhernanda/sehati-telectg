import configparser
from sqlalchemy import create_engine
from oauth2client.service_account import ServiceAccountCredentials
import gspread as gs
import gspread_dataframe as gd
from df2gspread import df2gspread as d2g
import pandas as pd
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import datetime 
import os
import sys 

config = configparser.ConfigParser()
config.read("../../config/"+ sys.argv[1] + ".ini")
print(config["db"]) 

POSTGRES_ADDRESS = (config["db"]["db_address"]) 
POSTGRES_PORT =  (config["db"]["db_port"])
POSTGRES_USERNAME = (config["db"]["db_username"])  
POSTGRES_PASSWORD = (config["db"]["db_password"])  
POSTGRES_DBNAME = (config["db"]["db_name"]) 

postgres_str = ('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'
                  .format(username=POSTGRES_USERNAME,
                   password=POSTGRES_PASSWORD,
                   ipaddress=POSTGRES_ADDRESS,
                   port=POSTGRES_PORT,
                   dbname=POSTGRES_DBNAME))
cnx = create_engine(postgres_str)

scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('../../config/Connect-gsheet.json', scope)
gc = gs.authorize(credentials)

bidan_1_maret = pd.read_sql_query('''
select 
	users.full_name, users.phone, users.email, users.date_of_birth,
	midwives.str_number, midwives.str_expiry_date, midwives.ibi_number, midwives.sipb_number,
	hg."name" as kind, 
	healthcares.id as tp_id, healthcares.code as tp_code, healthcares.name as tp_name, healthcares.region_id as tp_region_id,
	tp_districts.name as tp_district, tp_city.name as tp_city, tp_province.name as tp_province, 
	steering.id as parent_id, steering.code as parent_code, steering.name as parent_name, steering.region_id,
	steering_districts.name as steering_district, steering_city.name as steering_city, steering_province.name as steering_province, 
	works."owner", works."primary", works.deleted_at,
	case 
	when works.deleted_at is null
	  then 'false'
	else
	  'true'
	end as deleted
from users 
left join midwives on midwives.user_id = users.id 
left join works on works.user_id = users.id 
left join healthcares on healthcares.id = works.healthcare_id 
left join healthcare_groups hg on hg.id = healthcares.healthcare_group_id
left join regions tp_districts on tp_districts.id = healthcares.region_id 
left join regions tp_city on tp_city.id = tp_districts.parent_id 
left join regions tp_province on tp_province.id = tp_city.parent_id 
left join healthcares steering on steering.id = healthcares.parent_id 
left join regions steering_districts on steering_districts.id = steering.region_id 
left join regions steering_city on steering_city.id = steering_districts.parent_id 
left join regions steering_province on steering_province.id = steering_city.parent_id 
where role = 'midwife'
and (
	(users.created_at::date >= current_date - 1 or users.updated_at::date >= current_date - 1) 
	or (healthcares.created_at::date >= current_date - 1 or healthcares.updated_at::date >= current_date - 1) 
	or (works.created_at::date >= current_date - 1 or works.updated_at::date >= current_date - 1)
)
order by users.created_at desc 
''',cnx)

ws = gc.open("Data Bidan Verifikasi").worksheet("Bidan  > 1 Maret")
dataframe = pd.DataFrame(ws.get_all_records())
length = (len(dataframe.index) + 2)
gd.set_with_dataframe(ws, bidan_1_maret,row = length ) 
ws.delete_row(length)
