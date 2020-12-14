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

ws = gc.open(" ").worksheet(" ")
formula = ws.acell('O2', value_render_option='FORMULA').value
dataframe = pd.DataFrame(ws.get_all_records())
length = (len(dataframe.index) + 2)
colb = ('B'+str(length))
colc = ('C'+str(length))
cold = ('D'+str(length))
cole = ('E'+str(length))
colf = ('F'+str(length))
colg = ('G'+str(length))
colh = ('H'+str(length))
coli = ('I'+str(length))
colj = ('J'+str(length))
colk = ('K'+str(length))
coll = ('L'+str(length))
colm = ('M'+str(length))
col_a = ('A'+str(length))

ws.update_acell(colb, formula+'(RAW!$K:$K,RAW!$M:$M,'+col_a + ',RAW!$L:$L,B1)')
ws.update_acell(colc, formula+'(RAW!$K:$K,RAW!$M:$M,'+col_a + ',RAW!$L:$L,C1)')
ws.update_acell(cold, formula+'(RAW!$K:$K,RAW!$M:$M,'+col_a + ',RAW!$L:$L,D1)')
ws.update_acell(cole, formula+'(RAW!$K:$K,RAW!$M:$M,'+col_a + ',RAW!$L:$L,E1)')
ws.update_acell(colf, formula+'(RAW!$K:$K,RAW!$M:$M,'+col_a + ',RAW!$L:$L,F1)')
ws.update_acell(colg, formula+'(RAW!$K:$K,RAW!$M:$M,'+col_a + ',RAW!$L:$L,G1)')
ws.update_acell(colh, formula+'(RAW!$K:$K,RAW!$M:$M,'+col_a + ',RAW!$L:$L,H1)')
ws.update_acell(coli, formula+'(RAW!$K:$K,RAW!$M:$M,'+col_a + ',RAW!$L:$L,I1)')
ws.update_acell(colj, formula+'(RAW!$K:$K,RAW!$M:$M,'+col_a + ',RAW!$L:$L,J1)')
ws.update_acell(colk, formula+'(RAW!$K:$K,RAW!$M:$M,'+col_a + ',RAW!$L:$L,K1)')
ws.update_acell(coll, formula+'(RAW!$K:$K,RAW!$M:$M,'+col_a + ',RAW!$L:$L,L1)')
ws.update_acell(colm, formula+'(RAW!$K:$K,RAW!$M:$M,'+col_a + ',RAW!$L:$L,M1)')

ws = gc.open(" ").worksheet(" ")
dataframe = pd.DataFrame(ws.get_all_records())
length = (len(dataframe.index) + 2)
colb = ('B'+str(length))
colc = ('C'+str(length))
cold = ('D'+str(length))
cole = ('E'+str(length))
colf = ('F'+str(length))
colg = ('G'+str(length))
colh = ('H'+str(length))
coli = ('I'+str(length))
colj = ('J'+str(length))
colk = ('K'+str(length))
coll = ('L'+str(length))
colm = ('M'+str(length))
col_a = ('A'+str(length))
col_bb = ('B'+str(length + 1))
col_cc = ('C'+str(length + 1))
col_dd = ('D'+str(length + 1))
col_ee = ('E'+str(length + 1))
col_ff = ('F'+str(length + 1))
col_gg = ('G'+str(length + 1))
col_hh = ('H'+str(length + 1))
col_ii = ('I'+str(length + 1))
col_jj = ('J'+str(length + 1))
col_kk = ('K'+str(length + 1))
col_ll = ('L'+str(length + 1))
col_mm = ('M'+str(length + 1))

ws.update_acell(colb, '=(YoY!'+col_bb+'-YoY!'+colb+')/YoY!'+colb)
ws.update_acell(colc, '=(YoY!'+col_cc+'-YoY!'+colc+')/YoY!'+colc)
ws.update_acell(cold, '=(YoY!'+col_dd+'-YoY!'+cold+')/YoY!'+cold)
ws.update_acell(cole, '=(YoY!'+col_ee+'-YoY!'+cole+')/YoY!'+cole)
ws.update_acell(colf, '=(YoY!'+col_ff+'-YoY!'+colf+')/YoY!'+colf)
ws.update_acell(colg, '=(YoY!'+col_gg+'-YoY!'+colg+')/YoY!'+colg)
ws.update_acell(colh, '=(YoY!'+col_hh+'-YoY!'+colh+')/YoY!'+colh)
ws.update_acell(coli, '=(YoY!'+col_ii+'-YoY!'+coli+')/YoY!'+coli)
ws.update_acell(colj, '=(YoY!'+col_jj+'-YoY!'+colj+')/YoY!'+colj)
ws.update_acell(colk, '=(YoY!'+col_kk+'-YoY!'+colk+')/YoY!'+colk)
ws.update_acell(coll, '=(YoY!'+col_ll+'-YoY!'+coll+')/YoY!'+coll)
ws.update_acell(colm, '=(YoY!'+col_mm+'-YoY!'+colm+')/YoY!'+colm)

ws = gc.open(" ").worksheet(" ")
dataframe = pd.DataFrame(ws.get_all_records())
length = (len(dataframe.index) + 2)
colb = ('B'+str(length))
colc = ('C'+str(length))
cold = ('D'+str(length))
cole = ('E'+str(length))
colf = ('F'+str(length))
colg = ('G'+str(length))
colh = ('H'+str(length))
coli = ('I'+str(length))
colj = ('J'+str(length))
colk = ('K'+str(length))
coll = ('L'+str(length))
colm = ('M'+str(length))
col_a = ('A'+str(length))
col_bb = ('B'+str(length + 1))
col_dd = ('D'+str(length + 1))
col_ee = ('E'+str(length + 1))
col_gg = ('G'+str(length + 1))
col_hh = ('H'+str(length + 1))
col_jj = ('J'+str(length + 1))
col_kk = ('K'+str(length + 1))
col_mm = ('M'+str(length + 1))

ws.update_acell(colb, '=IFERROR((SUM(YoY!'+col_bb+':'+col_dd+')-SUM(YoY!'+colb+':'+cold+'))/SUM(YoY!'+colb+':'+cold+'),0)')
ws.update_acell(colc, '=IFERROR((SUM(YoY!'+col_ee+':'+col_gg+')-SUM(YoY!'+cole+':'+colg+'))/SUM(YoY!'+cole+':'+colg+'),0)')
ws.update_acell(cold, '=IFERROR((SUM(YoY!'+col_hh+':'+col_jj+')-SUM(YoY!'+colh+':'+colj+'))/SUM(YoY!'+colh+':'+colj+'),0)')
ws.update_acell(cole, '=IFERROR((SUM(YoY!'+col_kk+':'+col_mm+')-SUM(YoY!'+colk+':'+colm+'))/SUM(YoY!'+colk+':'+colm+'),0)')

ws = gc.open(" ").worksheet(" ")
dataframe = pd.DataFrame(ws.get_all_records())
length = (len(dataframe.index) + 2)
colb = ('B'+str(length))
colc = ('C'+str(length))
cold = ('D'+str(length))
cole = ('E'+str(length))
colf = ('F'+str(length))
colg = ('G'+str(length))
colh = ('H'+str(length))
coli = ('I'+str(length))
colj = ('J'+str(length))
colk = ('K'+str(length))
coll = ('L'+str(length))
colm = ('M'+str(length))
col_bb = ('B'+str(length + 1))
col_mm = ('M'+str(length + 1))

ws.update_acell(colb, '=IFERROR((SUM(YoY!'+col_bb+':'+col_mm+')-SUM(YoY!'+colb+':'+colm+'))/SUM(YoY!'+colb+':'+colm+'),0)')