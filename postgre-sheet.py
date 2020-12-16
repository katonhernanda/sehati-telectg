import configparser
from sqlalchemy import create_engine
from oauth2client.service_account import ServiceAccountCredentials
import gspread as gs
from df2gspread import df2gspread as d2g
import pandas as pd
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import datetime
import os
import sys
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

config_sendgrid = configparser.ConfigParser()
config_sendgrid.read("../../config/sendgrid.ini")
print(config_sendgrid["sendgrid"])

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

query = pd.read_sql_query ('''
''',cnx

ws1 = gc.open(" ").worksheet(" ")
ws1.clear()
set_with_dataframe(ws1, query)
