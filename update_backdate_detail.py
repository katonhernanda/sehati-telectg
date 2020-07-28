#!/usr/bin/env python
# coding: utf-8

# In[1]:


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
config.read("creden.ini")
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
credentials = ServiceAccountCredentials.from_json_keyfile_name('Connect-gsheet.json', scope)
gc = gs.authorize(credentials)
spredsheet_key = '1L1XA-7wJ511vMnV3Li_GA_kJQGnXeOoNGawDwcv9oc8'


# In[2]:


current = pd.read_sql_query('''
select distinct
prop.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(a.occurrence_date) as "Tgl Kunjungan", date(a.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when a.created_at is not null then 'ANC' end) as "Status",
concat(extract(month from a.created_at),'/',extract(year from a.created_at)) as bulan
from ancs a
	join reports on reports.reportable_id = a.id and reports.reportable_type = 'Sehati::Models::Anc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = a.healthcare_id
	join regions desa on desa.id = hv.region_id 
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions prop on prop.id = kota.parent_id
	join pregnancies p on p.id = a.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = a.midwife_id
where a.created_at + interval '1h'*desa.timezone >= date_trunc('month', CURRENT_DATE)
and a.occurrence_date < date(a.created_at)
and extract(month from a.occurrence_date) = extract(month from a.created_at) 
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(p2.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(p2.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when p.created_at is not null then 'PNC' end) as "Status",
concat(extract(month from p2.created_at),'/',extract(year from p2.created_at)) as bulan
from pncs p2 
	join reports on reports.reportable_id = p2.id and reports.reportable_type = 'Sehati::Models::Pnc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = p2.healthcare_id
	join regions desa on desa.id = hv.region_id
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = p2.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = p2.midwife_id
where (p2.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and p2.filled_at < date(p2.created_at)
and extract(month from p2.filled_at + interval '1h'*desa.timezone) = extract(month from (p2.created_at + interval '1h'*desa.timezone)) 
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(fo.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(fo.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when fo.created_at is not null then 'INC First Observation' end) as "Status",
concat(extract(month from fo.created_at),'/',extract(year from fo.created_at)) as bulan
from incs i
	join first_observations fo on fo.inc_id = i.id
	join reports on reports.reportable_id = i.id or reports.reportable_id = fo.id and reports.reportable_type = 'Sehati::Models::FirstObservation' or reports.reportable_type = 'Sehati::Models::Inc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = i.healthcare_id
	join regions desa on desa.id = hv.region_id
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = i.midwife_id
	where fo.filled_at is not null
 and (fo.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and fo.filled_at < date(fo.created_at)
and extract(month from (fo.filled_at + interval '1h'*desa.timezone)) = extract(month from (fo.created_at + interval '1h'*desa.timezone)) 
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(o.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(o.created_at + interval '1h'*desa.timezone) as "Tanggal Dibuat",
(case when o.created_at is not null then 'INC Observation' end) as "Status",
concat(extract(month from o.created_at),'/',extract(year from o.created_at)) as bulan
from incs i
	join observations o on o.inc_id = i.id
	join reports on reports.reportable_id = i.id or reports.reportable_id = o.id and reports.reportable_type = 'Sehati::Models::Observation' or reports.reportable_type = 'Sehati::Models::Inc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = i.healthcare_id
	join regions desa on desa.id = hv.region_id and desa."level" = 3
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = o.midwife_id
 where (o.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and o.filled_at <date(o.created_at)
and extract(month from (o.filled_at + interval '1h'*desa.timezone)) = extract(month from (o.created_at + interval '1h'*desa.timezone)) 
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
mi.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(m.maternity_time + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(m.created_at + interval '1h'*desa.timezone) as "Tanggal Dibuat",
(case when m.created_at is not null then 'INC Catatan Persalinian' end) as "Status",
concat(extract(month from m.created_at),'/',extract(year from m.created_at)) as bulan
from incs i
	join maternities m on m.inc_id = i.id
	join healthcares h on h.id = m.healthcare_id
	join regions desa on desa.id = m.region_id and desa."level" = 3
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users mi on mi.id = m.midwife_id
 where (m.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and m.maternity_time <date(m.created_at)
and extract(month from (m.maternity_time + interval '1h'*desa.timezone)) = extract(month from (m.created_at + interval '1h'*desa.timezone)) 
order by 9,10
''',cnx)
current

ws = gc.open("Laporan Back Data Detail").worksheet("Current_M")
dataframe = pd.DataFrame(ws.get_all_records())
length = (len(dataframe.index) + 2)
gd.set_with_dataframe(ws, current,row = length ) 
ws.delete_row(length)
# In[3]:


ws1 = gc.open("Laporan Back Data Detail").worksheet('Current_M')
ws1.clear()
set_with_dataframe(ws1, current)


# In[4]:


one_m_ago = pd.read_sql_query ('''
select distinct
prop.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(a.occurrence_date) as "Tgl Kunjungan", date(a.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when a.created_at is not null then 'ANC' end) as "Status",
concat(extract(month from a.created_at),'/',extract(year from a.created_at)) as bulan
from ancs a
	join reports on reports.reportable_id = a.id and reports.reportable_type = 'Sehati::Models::Anc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = a.healthcare_id
	join regions desa on desa.id = hv.region_id 
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions prop on prop.id = kota.parent_id
	join pregnancies p on p.id = a.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = a.midwife_id
where a.created_at + interval '1h'*desa.timezone >= date_trunc('month', CURRENT_DATE)
and a.occurrence_date < date(a.created_at)
and extract(month from a.occurrence_date) = extract(month from a.created_at) -1
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(p2.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(p2.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when p.created_at is not null then 'PNC' end) as "Status",
concat(extract(month from p2.created_at),'/',extract(year from p2.created_at)) as bulan
from pncs p2 
	join reports on reports.reportable_id = p2.id and reports.reportable_type = 'Sehati::Models::Pnc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = p2.healthcare_id
	join regions desa on desa.id = hv.region_id
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = p2.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = p2.midwife_id
where (p2.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and p2.filled_at < date(p2.created_at)
and extract(month from p2.filled_at + interval '1h'*desa.timezone) = extract(month from (p2.created_at + interval '1h'*desa.timezone)) -1
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(fo.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(fo.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when fo.created_at is not null then 'INC First Observation' end) as "Status",
concat(extract(month from fo.created_at),'/',extract(year from fo.created_at)) as bulan
from incs i
	join first_observations fo on fo.inc_id = i.id
	join reports on reports.reportable_id = i.id or reports.reportable_id = fo.id and reports.reportable_type = 'Sehati::Models::FirstObservation' or reports.reportable_type = 'Sehati::Models::Inc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = i.healthcare_id
	join regions desa on desa.id = hv.region_id
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = i.midwife_id
	where fo.filled_at is not null
 and (fo.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and fo.filled_at < date(fo.created_at)
and extract(month from (fo.filled_at + interval '1h'*desa.timezone)) = extract(month from (fo.created_at + interval '1h'*desa.timezone)) -1
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(o.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(o.created_at + interval '1h'*desa.timezone) as "Tanggal Dibuat",
(case when o.created_at is not null then 'INC Observation' end) as "Status",
concat(extract(month from o.created_at),'/',extract(year from o.created_at)) as bulan
from incs i
	join observations o on o.inc_id = i.id
	join reports on reports.reportable_id = i.id or reports.reportable_id = o.id and reports.reportable_type = 'Sehati::Models::Observation' or reports.reportable_type = 'Sehati::Models::Inc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = i.healthcare_id
	join regions desa on desa.id = hv.region_id and desa."level" = 3
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = o.midwife_id
 where (o.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and o.filled_at <date(o.created_at)
and extract(month from (o.filled_at + interval '1h'*desa.timezone)) = extract(month from (o.created_at + interval '1h'*desa.timezone)) -1
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
mi.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(m.maternity_time + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(m.created_at + interval '1h'*desa.timezone) as "Tanggal Dibuat",
(case when m.created_at is not null then 'INC Catatan Persalinian' end) as "Status",
concat(extract(month from m.created_at),'/',extract(year from m.created_at)) as bulan
from incs i
	join maternities m on m.inc_id = i.id
	join healthcares h on h.id = m.healthcare_id
	join regions desa on desa.id = m.region_id and desa."level" = 3
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users mi on mi.id = m.midwife_id
 where (m.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and m.maternity_time <date(m.created_at)
and extract(month from (m.maternity_time + interval '1h'*desa.timezone)) = extract(month from (m.created_at + interval '1h'*desa.timezone)) -1

order by 9,10
''',cnx)
one_m_ago


# In[5]:


ws = gc.open("Laporan Back Data Detail").worksheet("1M_ago")
dataframe = pd.DataFrame(ws.get_all_records())
length = (len(dataframe.index) + 2)
gd.set_with_dataframe(ws, one_m_ago,row = length ) 
ws.delete_row(length)

# In[6]:


two_m_ago = pd.read_sql_query ('''
select distinct
prop.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(a.occurrence_date) as "Tgl Kunjungan", date(a.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when a.created_at is not null then 'ANC' end) as "Status",
concat(extract(month from a.created_at),'/',extract(year from a.created_at)) as bulan
from ancs a
	join reports on reports.reportable_id = a.id and reports.reportable_type = 'Sehati::Models::Anc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = a.healthcare_id
	join regions desa on desa.id = hv.region_id 
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions prop on prop.id = kota.parent_id
	join pregnancies p on p.id = a.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = a.midwife_id
where a.created_at + interval '1h'*desa.timezone >= date_trunc('month', CURRENT_DATE)
and a.occurrence_date < date(a.created_at)
and extract(month from a.occurrence_date) = extract(month from a.created_at) -2
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(p2.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(p2.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when p.created_at is not null then 'PNC' end) as "Status",
concat(extract(month from p2.created_at),'/',extract(year from p2.created_at)) as bulan
from pncs p2 
	join reports on reports.reportable_id = p2.id and reports.reportable_type = 'Sehati::Models::Pnc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = p2.healthcare_id
	join regions desa on desa.id = hv.region_id
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = p2.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = p2.midwife_id
where (p2.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and p2.filled_at < date(p2.created_at)
and extract(month from p2.filled_at + interval '1h'*desa.timezone) = extract(month from (p2.created_at + interval '1h'*desa.timezone)) -2
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(fo.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(fo.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when fo.created_at is not null then 'INC First Observation' end) as "Status",
concat(extract(month from fo.created_at),'/',extract(year from fo.created_at)) as bulan
from incs i
	join first_observations fo on fo.inc_id = i.id
	join reports on reports.reportable_id = i.id or reports.reportable_id = fo.id and reports.reportable_type = 'Sehati::Models::FirstObservation' or reports.reportable_type = 'Sehati::Models::Inc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = i.healthcare_id
	join regions desa on desa.id = hv.region_id
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = i.midwife_id
	where fo.filled_at is not null
 and (fo.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and fo.filled_at < date(fo.created_at)
and extract(month from (fo.filled_at + interval '1h'*desa.timezone)) = extract(month from (fo.created_at + interval '1h'*desa.timezone)) -2
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(o.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(o.created_at + interval '1h'*desa.timezone) as "Tanggal Dibuat",
(case when o.created_at is not null then 'INC Observation' end) as "Status",
concat(extract(month from o.created_at),'/',extract(year from o.created_at)) as bulan
from incs i
	join observations o on o.inc_id = i.id
	join reports on reports.reportable_id = i.id or reports.reportable_id = o.id and reports.reportable_type = 'Sehati::Models::Observation' or reports.reportable_type = 'Sehati::Models::Inc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = i.healthcare_id
	join regions desa on desa.id = hv.region_id and desa."level" = 3
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = o.midwife_id
 where (o.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and o.filled_at <date(o.created_at)
and extract(month from (o.filled_at + interval '1h'*desa.timezone)) = extract(month from (o.created_at + interval '1h'*desa.timezone)) -2
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
mi.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(m.maternity_time + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(m.created_at + interval '1h'*desa.timezone) as "Tanggal Dibuat",
(case when m.created_at is not null then 'INC Catatan Persalinian' end) as "Status",
concat(extract(month from m.created_at),'/',extract(year from m.created_at)) as bulan
from incs i
	join maternities m on m.inc_id = i.id
	join healthcares h on h.id = m.healthcare_id
	join regions desa on desa.id = m.region_id and desa."level" = 3
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users mi on mi.id = m.midwife_id
 where (m.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and m.maternity_time <date(m.created_at)
and extract(month from (m.maternity_time + interval '1h'*desa.timezone)) = extract(month from (m.created_at + interval '1h'*desa.timezone)) -2
order by 9,10
''',cnx)
two_m_ago


# In[7]:

ws = gc.open("Laporan Back Data Detail").worksheet("2M_ago")
dataframe = pd.DataFrame(ws.get_all_records())
length = (len(dataframe.index) + 2)
gd.set_with_dataframe(ws, two_m_ago,row = length ) 
ws.delete_row(length)


# In[8]:


three_m_ago = pd.read_sql_query ('''
select distinct
prop.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(a.occurrence_date) as "Tgl Kunjungan", date(a.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when a.created_at is not null then 'ANC' end) as "Status",
concat(extract(month from a.created_at),'/',extract(year from a.created_at)) as bulan
from ancs a
	join reports on reports.reportable_id = a.id and reports.reportable_type = 'Sehati::Models::Anc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = a.healthcare_id
	join regions desa on desa.id = hv.region_id 
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions prop on prop.id = kota.parent_id
	join pregnancies p on p.id = a.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = a.midwife_id
where a.created_at + interval '1h'*desa.timezone >= date_trunc('month', CURRENT_DATE)
and a.occurrence_date < date(a.created_at)
and extract(month from a.occurrence_date) = extract(month from a.created_at) -3
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(p2.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(p2.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when p.created_at is not null then 'PNC' end) as "Status",
concat(extract(month from p2.created_at),'/',extract(year from p2.created_at)) as bulan
from pncs p2 
	join reports on reports.reportable_id = p2.id and reports.reportable_type = 'Sehati::Models::Pnc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = p2.healthcare_id
	join regions desa on desa.id = hv.region_id
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = p2.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = p2.midwife_id
where (p2.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and p2.filled_at < date(p2.created_at)
and extract(month from p2.filled_at + interval '1h'*desa.timezone) = extract(month from (p2.created_at + interval '1h'*desa.timezone)) -3
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(fo.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(fo.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when fo.created_at is not null then 'INC First Observation' end) as "Status",
concat(extract(month from fo.created_at),'/',extract(year from fo.created_at)) as bulan
from incs i
	join first_observations fo on fo.inc_id = i.id
	join reports on reports.reportable_id = i.id or reports.reportable_id = fo.id and reports.reportable_type = 'Sehati::Models::FirstObservation' or reports.reportable_type = 'Sehati::Models::Inc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = i.healthcare_id
	join regions desa on desa.id = hv.region_id
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = i.midwife_id
	where fo.filled_at is not null
 and (fo.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and fo.filled_at < date(fo.created_at)
and extract(month from (fo.filled_at + interval '1h'*desa.timezone)) = extract(month from (fo.created_at + interval '1h'*desa.timezone)) -3
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(o.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(o.created_at + interval '1h'*desa.timezone) as "Tanggal Dibuat",
(case when o.created_at is not null then 'INC Observation' end) as "Status",
concat(extract(month from o.created_at),'/',extract(year from o.created_at)) as bulan
from incs i
	join observations o on o.inc_id = i.id
	join reports on reports.reportable_id = i.id or reports.reportable_id = o.id and reports.reportable_type = 'Sehati::Models::Observation' or reports.reportable_type = 'Sehati::Models::Inc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = i.healthcare_id
	join regions desa on desa.id = hv.region_id and desa."level" = 3
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = o.midwife_id
 where (o.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and o.filled_at <date(o.created_at)
and extract(month from (o.filled_at + interval '1h'*desa.timezone)) = extract(month from (o.created_at + interval '1h'*desa.timezone)) -3
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
mi.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(m.maternity_time + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(m.created_at + interval '1h'*desa.timezone) as "Tanggal Dibuat",
(case when m.created_at is not null then 'INC Catatan Persalinian' end) as "Status",
concat(extract(month from m.created_at),'/',extract(year from m.created_at)) as bulan
from incs i
	join maternities m on m.inc_id = i.id
	join healthcares h on h.id = m.healthcare_id
	join regions desa on desa.id = m.region_id and desa."level" = 3
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users mi on mi.id = m.midwife_id
 where (m.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and m.maternity_time <date(m.created_at)
and extract(month from (m.maternity_time + interval '1h'*desa.timezone)) = extract(month from (m.created_at + interval '1h'*desa.timezone)) -3
order by 9,10
''',cnx)
three_m_ago


# In[9]:


ws = gc.open("Laporan Back Data Detail").worksheet("3M_ago")
dataframe = pd.DataFrame(ws.get_all_records())
length = (len(dataframe.index) + 2)
gd.set_with_dataframe(ws,three_m_ago,row = length ) 
ws.delete_row(length)


# In[10]:


four_m_ago = pd.read_sql_query ('''
select distinct
prop.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(a.occurrence_date) as "Tgl Kunjungan", date(a.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when a.created_at is not null then 'ANC' end) as "Status",
concat(extract(month from a.created_at),'/',extract(year from a.created_at)) as bulan
from ancs a
	join reports on reports.reportable_id = a.id and reports.reportable_type = 'Sehati::Models::Anc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = a.healthcare_id
	join regions desa on desa.id = hv.region_id 
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions prop on prop.id = kota.parent_id
	join pregnancies p on p.id = a.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = a.midwife_id
where a.created_at + interval '1h'*desa.timezone >= date_trunc('month', CURRENT_DATE)
and a.occurrence_date < date(a.created_at)
and extract(month from a.occurrence_date) = extract(month from a.created_at) -4
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(p2.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(p2.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when p.created_at is not null then 'PNC' end) as "Status",
concat(extract(month from p2.created_at),'/',extract(year from p2.created_at)) as bulan
from pncs p2 
	join reports on reports.reportable_id = p2.id and reports.reportable_type = 'Sehati::Models::Pnc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = p2.healthcare_id
	join regions desa on desa.id = hv.region_id
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = p2.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = p2.midwife_id
where (p2.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and p2.filled_at < date(p2.created_at)
and extract(month from p2.filled_at + interval '1h'*desa.timezone) = extract(month from (p2.created_at + interval '1h'*desa.timezone)) -4
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(fo.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(fo.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when fo.created_at is not null then 'INC First Observation' end) as "Status",
concat(extract(month from fo.created_at),'/',extract(year from fo.created_at)) as bulan
from incs i
	join first_observations fo on fo.inc_id = i.id
	join reports on reports.reportable_id = i.id or reports.reportable_id = fo.id and reports.reportable_type = 'Sehati::Models::FirstObservation' or reports.reportable_type = 'Sehati::Models::Inc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = i.healthcare_id
	join regions desa on desa.id = hv.region_id
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = i.midwife_id
	where fo.filled_at is not null
 and (fo.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and fo.filled_at < date(fo.created_at)
and extract(month from (fo.filled_at + interval '1h'*desa.timezone)) = extract(month from (fo.created_at + interval '1h'*desa.timezone)) -4
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(o.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(o.created_at + interval '1h'*desa.timezone) as "Tanggal Dibuat",
(case when o.created_at is not null then 'INC Observation' end) as "Status",
concat(extract(month from o.created_at),'/',extract(year from o.created_at)) as bulan
from incs i
	join observations o on o.inc_id = i.id
	join reports on reports.reportable_id = i.id or reports.reportable_id = o.id and reports.reportable_type = 'Sehati::Models::Observation' or reports.reportable_type = 'Sehati::Models::Inc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = i.healthcare_id
	join regions desa on desa.id = hv.region_id and desa."level" = 3
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = o.midwife_id
 where (o.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and o.filled_at <date(o.created_at)
and extract(month from (o.filled_at + interval '1h'*desa.timezone)) = extract(month from (o.created_at + interval '1h'*desa.timezone)) -4
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
mi.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(m.maternity_time + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(m.created_at + interval '1h'*desa.timezone) as "Tanggal Dibuat",
(case when m.created_at is not null then 'INC Catatan Persalinian' end) as "Status",
concat(extract(month from m.created_at),'/',extract(year from m.created_at)) as bulan
from incs i
	join maternities m on m.inc_id = i.id
	join healthcares h on h.id = m.healthcare_id
	join regions desa on desa.id = m.region_id and desa."level" = 3
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users mi on mi.id = m.midwife_id
 where (m.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and m.maternity_time <date(m.created_at)
and extract(month from (m.maternity_time + interval '1h'*desa.timezone)) = extract(month from (m.created_at + interval '1h'*desa.timezone)) -4
order by 9,10
''',cnx)
four_m_ago


# In[11]:


ws = gc.open("Laporan Back Data Detail").worksheet("4M_ago")
dataframe = pd.DataFrame(ws.get_all_records())
length = (len(dataframe.index) + 2)
gd.set_with_dataframe(ws, four_m_ago,row = length ) 
ws.delete_row(length)


# In[12]:


five_m_ago = pd.read_sql_query ('''
select distinct
prop.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(a.occurrence_date) as "Tgl Kunjungan", date(a.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when a.created_at is not null then 'ANC' end) as "Status",
concat(extract(month from a.created_at),'/',extract(year from a.created_at)) as bulan
from ancs a
	join reports on reports.reportable_id = a.id and reports.reportable_type = 'Sehati::Models::Anc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = a.healthcare_id
	join regions desa on desa.id = hv.region_id 
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions prop on prop.id = kota.parent_id
	join pregnancies p on p.id = a.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = a.midwife_id
where a.created_at + interval '1h'*desa.timezone >= date_trunc('month', CURRENT_DATE)
and a.occurrence_date < date(a.created_at)
and extract(month from a.occurrence_date) = extract(month from a.created_at) -5
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(p2.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(p2.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when p.created_at is not null then 'PNC' end) as "Status",
concat(extract(month from p2.created_at),'/',extract(year from p2.created_at)) as bulan
from pncs p2 
	join reports on reports.reportable_id = p2.id and reports.reportable_type = 'Sehati::Models::Pnc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = p2.healthcare_id
	join regions desa on desa.id = hv.region_id
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = p2.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = p2.midwife_id
where (p2.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and p2.filled_at < date(p2.created_at)
and extract(month from p2.filled_at + interval '1h'*desa.timezone) = extract(month from (p2.created_at + interval '1h'*desa.timezone)) -5
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(fo.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(fo.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when fo.created_at is not null then 'INC First Observation' end) as "Status",
concat(extract(month from fo.created_at),'/',extract(year from fo.created_at)) as bulan
from incs i
	join first_observations fo on fo.inc_id = i.id
	join reports on reports.reportable_id = i.id or reports.reportable_id = fo.id and reports.reportable_type = 'Sehati::Models::FirstObservation' or reports.reportable_type = 'Sehati::Models::Inc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = i.healthcare_id
	join regions desa on desa.id = hv.region_id
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = i.midwife_id
	where fo.filled_at is not null
 and (fo.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and fo.filled_at < date(fo.created_at)
and extract(month from (fo.filled_at + interval '1h'*desa.timezone)) = extract(month from (fo.created_at + interval '1h'*desa.timezone)) -5
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(o.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(o.created_at + interval '1h'*desa.timezone) as "Tanggal Dibuat",
(case when o.created_at is not null then 'INC Observation' end) as "Status",
concat(extract(month from o.created_at),'/',extract(year from o.created_at)) as bulan
from incs i
	join observations o on o.inc_id = i.id
	join reports on reports.reportable_id = i.id or reports.reportable_id = o.id and reports.reportable_type = 'Sehati::Models::Observation' or reports.reportable_type = 'Sehati::Models::Inc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = i.healthcare_id
	join regions desa on desa.id = hv.region_id and desa."level" = 3
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = o.midwife_id
 where (o.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and o.filled_at <date(o.created_at)
and extract(month from (o.filled_at + interval '1h'*desa.timezone)) = extract(month from (o.created_at + interval '1h'*desa.timezone)) -5
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
mi.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(m.maternity_time + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(m.created_at + interval '1h'*desa.timezone) as "Tanggal Dibuat",
(case when m.created_at is not null then 'INC Catatan Persalinian' end) as "Status",
concat(extract(month from m.created_at),'/',extract(year from m.created_at)) as bulan
from incs i
	join maternities m on m.inc_id = i.id
	join healthcares h on h.id = m.healthcare_id
	join regions desa on desa.id = m.region_id and desa."level" = 3
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users mi on mi.id = m.midwife_id
 where (m.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and m.maternity_time <date(m.created_at)
and extract(month from (m.maternity_time + interval '1h'*desa.timezone)) = extract(month from (m.created_at + interval '1h'*desa.timezone)) -5
order by 9,10
''',cnx)
five_m_ago


# In[13]:

ws = gc.open("Laporan Back Data Detail").worksheet("5M_ago")
dataframe = pd.DataFrame(ws.get_all_records())
length = (len(dataframe.index) + 2)
gd.set_with_dataframe(ws, five_m_ago,row = length ) 
ws.delete_row(length)


# In[14]:


six_m_ago = pd.read_sql_query ('''
select distinct
prop.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(a.occurrence_date) as "Tgl Kunjungan", date(a.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when a.created_at is not null then 'ANC' end) as "Status",
concat(extract(month from a.created_at),'/',extract(year from a.created_at)) as bulan
from ancs a
	join reports on reports.reportable_id = a.id and reports.reportable_type = 'Sehati::Models::Anc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = a.healthcare_id
	join regions desa on desa.id = hv.region_id 
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions prop on prop.id = kota.parent_id
	join pregnancies p on p.id = a.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = a.midwife_id
where a.created_at + interval '1h'*desa.timezone >= date_trunc('month', CURRENT_DATE)
and a.occurrence_date < date(a.created_at)
and extract(month from a.occurrence_date) = extract(month from a.created_at) -6
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(p2.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(p2.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when p.created_at is not null then 'PNC' end) as "Status",
concat(extract(month from p2.created_at),'/',extract(year from p2.created_at)) as bulan
from pncs p2 
	join reports on reports.reportable_id = p2.id and reports.reportable_type = 'Sehati::Models::Pnc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = p2.healthcare_id
	join regions desa on desa.id = hv.region_id
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = p2.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = p2.midwife_id
where (p2.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and p2.filled_at < date(p2.created_at)
and extract(month from p2.filled_at + interval '1h'*desa.timezone) = extract(month from (p2.created_at + interval '1h'*desa.timezone)) -6
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(fo.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(fo.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when fo.created_at is not null then 'INC First Observation' end) as "Status",
concat(extract(month from fo.created_at),'/',extract(year from fo.created_at)) as bulan
from incs i
	join first_observations fo on fo.inc_id = i.id
	join reports on reports.reportable_id = i.id or reports.reportable_id = fo.id and reports.reportable_type = 'Sehati::Models::FirstObservation' or reports.reportable_type = 'Sehati::Models::Inc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = i.healthcare_id
	join regions desa on desa.id = hv.region_id
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = i.midwife_id
	where fo.filled_at is not null
 and (fo.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and fo.filled_at < date(fo.created_at)
and extract(month from (fo.filled_at + interval '1h'*desa.timezone)) = extract(month from (fo.created_at + interval '1h'*desa.timezone)) -6
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(o.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(o.created_at + interval '1h'*desa.timezone) as "Tanggal Dibuat",
(case when o.created_at is not null then 'INC Observation' end) as "Status",
concat(extract(month from o.created_at),'/',extract(year from o.created_at)) as bulan
from incs i
	join observations o on o.inc_id = i.id
	join reports on reports.reportable_id = i.id or reports.reportable_id = o.id and reports.reportable_type = 'Sehati::Models::Observation' or reports.reportable_type = 'Sehati::Models::Inc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = i.healthcare_id
	join regions desa on desa.id = hv.region_id and desa."level" = 3
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = o.midwife_id
 where (o.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and o.filled_at <date(o.created_at)
and extract(month from (o.filled_at + interval '1h'*desa.timezone)) = extract(month from (o.created_at + interval '1h'*desa.timezone)) -6
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
mi.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(m.maternity_time + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(m.created_at + interval '1h'*desa.timezone) as "Tanggal Dibuat",
(case when m.created_at is not null then 'INC Catatan Persalinian' end) as "Status",
concat(extract(month from m.created_at),'/',extract(year from m.created_at)) as bulan
from incs i
	join maternities m on m.inc_id = i.id
	join healthcares h on h.id = m.healthcare_id
	join regions desa on desa.id = m.region_id and desa."level" = 3
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users mi on mi.id = m.midwife_id
 where (m.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and m.maternity_time <date(m.created_at)
and extract(month from (m.maternity_time + interval '1h'*desa.timezone)) = extract(month from (m.created_at + interval '1h'*desa.timezone)) -6
order by 9,10
''',cnx)
six_m_ago


# In[15]:

ws = gc.open("Laporan Back Data Detail").worksheet("6M_ago")
dataframe = pd.DataFrame(ws.get_all_records())
length = (len(dataframe.index) + 2)
gd.set_with_dataframe(ws, six_m_ago,row = length ) 
ws.delete_row(length)


# In[16]:


seven_m_ago = pd.read_sql_query ('''
select distinct
prop.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(a.occurrence_date) as "Tgl Kunjungan", date(a.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when a.created_at is not null then 'ANC' end) as "Status",
concat(extract(month from a.created_at),'/',extract(year from a.created_at)) as bulan
from ancs a
	join reports on reports.reportable_id = a.id and reports.reportable_type = 'Sehati::Models::Anc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = a.healthcare_id
	join regions desa on desa.id = hv.region_id 
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions prop on prop.id = kota.parent_id
	join pregnancies p on p.id = a.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = a.midwife_id
where a.created_at + interval '1h'*desa.timezone >= date_trunc('month', CURRENT_DATE)
and a.occurrence_date < date(a.created_at)
and extract(month from a.occurrence_date) = extract(month from a.created_at) -7
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(p2.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(p2.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when p.created_at is not null then 'PNC' end) as "Status",
concat(extract(month from p2.created_at),'/',extract(year from p2.created_at)) as bulan
from pncs p2 
	join reports on reports.reportable_id = p2.id and reports.reportable_type = 'Sehati::Models::Pnc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = p2.healthcare_id
	join regions desa on desa.id = hv.region_id
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = p2.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = p2.midwife_id
where (p2.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and p2.filled_at < date(p2.created_at)
and extract(month from p2.filled_at + interval '1h'*desa.timezone) = extract(month from (p2.created_at + interval '1h'*desa.timezone)) -7
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(fo.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(fo.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when fo.created_at is not null then 'INC First Observation' end) as "Status",
concat(extract(month from fo.created_at),'/',extract(year from fo.created_at)) as bulan
from incs i
	join first_observations fo on fo.inc_id = i.id
	join reports on reports.reportable_id = i.id or reports.reportable_id = fo.id and reports.reportable_type = 'Sehati::Models::FirstObservation' or reports.reportable_type = 'Sehati::Models::Inc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = i.healthcare_id
	join regions desa on desa.id = hv.region_id
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = i.midwife_id
	where fo.filled_at is not null
 and (fo.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and fo.filled_at < date(fo.created_at)
and extract(month from (fo.filled_at + interval '1h'*desa.timezone)) = extract(month from (fo.created_at + interval '1h'*desa.timezone)) -7
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(o.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(o.created_at + interval '1h'*desa.timezone) as "Tanggal Dibuat",
(case when o.created_at is not null then 'INC Observation' end) as "Status",
concat(extract(month from o.created_at),'/',extract(year from o.created_at)) as bulan
from incs i
	join observations o on o.inc_id = i.id
	join reports on reports.reportable_id = i.id or reports.reportable_id = o.id and reports.reportable_type = 'Sehati::Models::Observation' or reports.reportable_type = 'Sehati::Models::Inc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = i.healthcare_id
	join regions desa on desa.id = hv.region_id and desa."level" = 3
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = o.midwife_id
 where (o.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and o.filled_at <date(o.created_at)
and extract(month from (o.filled_at + interval '1h'*desa.timezone)) = extract(month from (o.created_at + interval '1h'*desa.timezone)) -7
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
mi.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(m.maternity_time + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(m.created_at + interval '1h'*desa.timezone) as "Tanggal Dibuat",
(case when m.created_at is not null then 'INC Catatan Persalinian' end) as "Status",
concat(extract(month from m.created_at),'/',extract(year from m.created_at)) as bulan
from incs i
	join maternities m on m.inc_id = i.id
	join healthcares h on h.id = m.healthcare_id
	join regions desa on desa.id = m.region_id and desa."level" = 3
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users mi on mi.id = m.midwife_id
 where (m.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and m.maternity_time <date(m.created_at)
and extract(month from (m.maternity_time + interval '1h'*desa.timezone)) = extract(month from (m.created_at + interval '1h'*desa.timezone)) -7
order by 9,10
''',cnx)
seven_m_ago


# In[17]:


ws = gc.open("Laporan Back Data Detail").worksheet("7M_ago")
dataframe = pd.DataFrame(ws.get_all_records())
length = (len(dataframe.index) + 2)
gd.set_with_dataframe(ws, seven_m_ago,row = length ) 
ws.delete_row(length)


# In[18]:


eight_m_ago = pd.read_sql_query ('''
select distinct
prop.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(a.occurrence_date) as "Tgl Kunjungan", date(a.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when a.created_at is not null then 'ANC' end) as "Status",
concat(extract(month from a.created_at),'/',extract(year from a.created_at)) as bulan
from ancs a
	join reports on reports.reportable_id = a.id and reports.reportable_type = 'Sehati::Models::Anc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = a.healthcare_id
	join regions desa on desa.id = hv.region_id 
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions prop on prop.id = kota.parent_id
	join pregnancies p on p.id = a.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = a.midwife_id
where a.created_at + interval '1h'*desa.timezone >= date_trunc('month', CURRENT_DATE)
and a.occurrence_date < date(a.created_at)
and extract(month from a.occurrence_date) = extract(month from a.created_at) -8
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(p2.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(p2.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when p.created_at is not null then 'PNC' end) as "Status",
concat(extract(month from p2.created_at),'/',extract(year from p2.created_at)) as bulan
from pncs p2 
	join reports on reports.reportable_id = p2.id and reports.reportable_type = 'Sehati::Models::Pnc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = p2.healthcare_id
	join regions desa on desa.id = hv.region_id
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = p2.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = p2.midwife_id
where (p2.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and p2.filled_at < date(p2.created_at)
and extract(month from p2.filled_at + interval '1h'*desa.timezone) = extract(month from (p2.created_at + interval '1h'*desa.timezone)) -8
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(fo.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(fo.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when fo.created_at is not null then 'INC First Observation' end) as "Status",
concat(extract(month from fo.created_at),'/',extract(year from fo.created_at)) as bulan
from incs i
	join first_observations fo on fo.inc_id = i.id
	join reports on reports.reportable_id = i.id or reports.reportable_id = fo.id and reports.reportable_type = 'Sehati::Models::FirstObservation' or reports.reportable_type = 'Sehati::Models::Inc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = i.healthcare_id
	join regions desa on desa.id = hv.region_id
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = i.midwife_id
	where fo.filled_at is not null
 and (fo.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and fo.filled_at < date(fo.created_at)
and extract(month from (fo.filled_at + interval '1h'*desa.timezone)) = extract(month from (fo.created_at + interval '1h'*desa.timezone)) -8
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(o.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(o.created_at + interval '1h'*desa.timezone) as "Tanggal Dibuat",
(case when o.created_at is not null then 'INC Observation' end) as "Status",
concat(extract(month from o.created_at),'/',extract(year from o.created_at)) as bulan
from incs i
	join observations o on o.inc_id = i.id
	join reports on reports.reportable_id = i.id or reports.reportable_id = o.id and reports.reportable_type = 'Sehati::Models::Observation' or reports.reportable_type = 'Sehati::Models::Inc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = i.healthcare_id
	join regions desa on desa.id = hv.region_id and desa."level" = 3
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = o.midwife_id
 where (o.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and o.filled_at <date(o.created_at)
and extract(month from (o.filled_at + interval '1h'*desa.timezone)) = extract(month from (o.created_at + interval '1h'*desa.timezone)) -8
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
mi.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(m.maternity_time + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(m.created_at + interval '1h'*desa.timezone) as "Tanggal Dibuat",
(case when m.created_at is not null then 'INC Catatan Persalinian' end) as "Status",
concat(extract(month from m.created_at),'/',extract(year from m.created_at)) as bulan
from incs i
	join maternities m on m.inc_id = i.id
	join healthcares h on h.id = m.healthcare_id
	join regions desa on desa.id = m.region_id and desa."level" = 3
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users mi on mi.id = m.midwife_id
 where (m.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and m.maternity_time <date(m.created_at)
and extract(month from (m.maternity_time + interval '1h'*desa.timezone)) = extract(month from (m.created_at + interval '1h'*desa.timezone)) -8
order by 9,10
''',cnx)
eight_m_ago


# In[19]:


ws = gc.open("Laporan Back Data Detail").worksheet("8M_ago")
dataframe = pd.DataFrame(ws.get_all_records())
length = (len(dataframe.index) + 2)
gd.set_with_dataframe(ws, eight_m_ago,row = length ) 
ws.delete_row(length)


# In[20]:


nine_m_ago = pd.read_sql_query ('''
select distinct
prop.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(a.occurrence_date) as "Tgl Kunjungan", date(a.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when a.created_at is not null then 'ANC' end) as "Status",
concat(extract(month from a.created_at),'/',extract(year from a.created_at)) as bulan
from ancs a
	join reports on reports.reportable_id = a.id and reports.reportable_type = 'Sehati::Models::Anc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = a.healthcare_id
	join regions desa on desa.id = hv.region_id 
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions prop on prop.id = kota.parent_id
	join pregnancies p on p.id = a.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = a.midwife_id
where a.created_at + interval '1h'*desa.timezone >= date_trunc('month', CURRENT_DATE)
and a.occurrence_date < date(a.created_at)
and extract(month from a.occurrence_date) = extract(month from a.created_at) -9
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(p2.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(p2.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when p.created_at is not null then 'PNC' end) as "Status",
concat(extract(month from p2.created_at),'/',extract(year from p2.created_at)) as bulan
from pncs p2 
	join reports on reports.reportable_id = p2.id and reports.reportable_type = 'Sehati::Models::Pnc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = p2.healthcare_id
	join regions desa on desa.id = hv.region_id
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = p2.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = p2.midwife_id
where (p2.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and p2.filled_at < date(p2.created_at)
and extract(month from p2.filled_at + interval '1h'*desa.timezone) = extract(month from (p2.created_at + interval '1h'*desa.timezone)) -9
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(fo.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(fo.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when fo.created_at is not null then 'INC First Observation' end) as "Status",
concat(extract(month from fo.created_at),'/',extract(year from fo.created_at)) as bulan
from incs i
	join first_observations fo on fo.inc_id = i.id
	join reports on reports.reportable_id = i.id or reports.reportable_id = fo.id and reports.reportable_type = 'Sehati::Models::FirstObservation' or reports.reportable_type = 'Sehati::Models::Inc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = i.healthcare_id
	join regions desa on desa.id = hv.region_id
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = i.midwife_id
	where fo.filled_at is not null
 and (fo.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and fo.filled_at < date(fo.created_at)
and extract(month from (fo.filled_at + interval '1h'*desa.timezone)) = extract(month from (fo.created_at + interval '1h'*desa.timezone)) -9
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(o.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(o.created_at + interval '1h'*desa.timezone) as "Tanggal Dibuat",
(case when o.created_at is not null then 'INC Observation' end) as "Status",
concat(extract(month from o.created_at),'/',extract(year from o.created_at)) as bulan
from incs i
	join observations o on o.inc_id = i.id
	join reports on reports.reportable_id = i.id or reports.reportable_id = o.id and reports.reportable_type = 'Sehati::Models::Observation' or reports.reportable_type = 'Sehati::Models::Inc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = i.healthcare_id
	join regions desa on desa.id = hv.region_id and desa."level" = 3
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = o.midwife_id
 where (o.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and o.filled_at <date(o.created_at)
and extract(month from (o.filled_at + interval '1h'*desa.timezone)) = extract(month from (o.created_at + interval '1h'*desa.timezone)) -9
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
mi.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(m.maternity_time + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(m.created_at + interval '1h'*desa.timezone) as "Tanggal Dibuat",
(case when m.created_at is not null then 'INC Catatan Persalinian' end) as "Status",
concat(extract(month from m.created_at),'/',extract(year from m.created_at)) as bulan
from incs i
	join maternities m on m.inc_id = i.id
	join healthcares h on h.id = m.healthcare_id
	join regions desa on desa.id = m.region_id and desa."level" = 3
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users mi on mi.id = m.midwife_id
 where (m.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and m.maternity_time <date(m.created_at)
and extract(month from (m.maternity_time + interval '1h'*desa.timezone)) = extract(month from (m.created_at + interval '1h'*desa.timezone)) -9
order by 9,10
''',cnx)
nine_m_ago


# In[21]:


ws = gc.open("Laporan Back Data Detail").worksheet("9M_ago")
dataframe = pd.DataFrame(ws.get_all_records())
length = (len(dataframe.index) + 2)
gd.set_with_dataframe(ws, nine_m_ago,row = length ) 
ws.delete_row(length)


# In[22]:


ten_m_ago = pd.read_sql_query ('''
select distinct
prop.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(a.occurrence_date) as "Tgl Kunjungan", date(a.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when a.created_at is not null then 'ANC' end) as "Status",
concat(extract(month from a.created_at),'/',extract(year from a.created_at)) as bulan
from ancs a
	join reports on reports.reportable_id = a.id and reports.reportable_type = 'Sehati::Models::Anc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = a.healthcare_id
	join regions desa on desa.id = hv.region_id 
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions prop on prop.id = kota.parent_id
	join pregnancies p on p.id = a.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = a.midwife_id
where a.created_at + interval '1h'*desa.timezone >= date_trunc('month', CURRENT_DATE)
and a.occurrence_date < date(a.created_at)
and extract(month from a.occurrence_date) = extract(month from a.created_at) -10
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(p2.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(p2.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when p.created_at is not null then 'PNC' end) as "Status",
concat(extract(month from p2.created_at),'/',extract(year from p2.created_at)) as bulan
from pncs p2 
	join reports on reports.reportable_id = p2.id and reports.reportable_type = 'Sehati::Models::Pnc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = p2.healthcare_id
	join regions desa on desa.id = hv.region_id
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = p2.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = p2.midwife_id
where (p2.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and p2.filled_at < date(p2.created_at)
and extract(month from p2.filled_at + interval '1h'*desa.timezone) = extract(month from (p2.created_at + interval '1h'*desa.timezone)) -10
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(fo.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(fo.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when fo.created_at is not null then 'INC First Observation' end) as "Status",
concat(extract(month from fo.created_at),'/',extract(year from fo.created_at)) as bulan
from incs i
	join first_observations fo on fo.inc_id = i.id
	join reports on reports.reportable_id = i.id or reports.reportable_id = fo.id and reports.reportable_type = 'Sehati::Models::FirstObservation' or reports.reportable_type = 'Sehati::Models::Inc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = i.healthcare_id
	join regions desa on desa.id = hv.region_id
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = i.midwife_id
	where fo.filled_at is not null
 and (fo.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and fo.filled_at < date(fo.created_at)
and extract(month from (fo.filled_at + interval '1h'*desa.timezone)) = extract(month from (fo.created_at + interval '1h'*desa.timezone)) -10
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(o.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(o.created_at + interval '1h'*desa.timezone) as "Tanggal Dibuat",
(case when o.created_at is not null then 'INC Observation' end) as "Status",
concat(extract(month from o.created_at),'/',extract(year from o.created_at)) as bulan
from incs i
	join observations o on o.inc_id = i.id
	join reports on reports.reportable_id = i.id or reports.reportable_id = o.id and reports.reportable_type = 'Sehati::Models::Observation' or reports.reportable_type = 'Sehati::Models::Inc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = i.healthcare_id
	join regions desa on desa.id = hv.region_id and desa."level" = 3
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = o.midwife_id
 where (o.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and o.filled_at <date(o.created_at)
and extract(month from (o.filled_at + interval '1h'*desa.timezone)) = extract(month from (o.created_at + interval '1h'*desa.timezone)) -10
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
mi.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(m.maternity_time + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(m.created_at + interval '1h'*desa.timezone) as "Tanggal Dibuat",
(case when m.created_at is not null then 'INC Catatan Persalinian' end) as "Status",
concat(extract(month from m.created_at),'/',extract(year from m.created_at)) as bulan
from incs i
	join maternities m on m.inc_id = i.id
	join healthcares h on h.id = m.healthcare_id
	join regions desa on desa.id = m.region_id and desa."level" = 3
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users mi on mi.id = m.midwife_id
 where (m.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and m.maternity_time <date(m.created_at)
and extract(month from (m.maternity_time + interval '1h'*desa.timezone)) = extract(month from (m.created_at + interval '1h'*desa.timezone)) -10
order by 9,10
''',cnx)
ten_m_ago


# In[23]:

ws = gc.open("Laporan Back Data Detail").worksheet("10M_ago")
dataframe = pd.DataFrame(ws.get_all_records())
length = (len(dataframe.index) + 2)
gd.set_with_dataframe(ws, ten_m_ago,row = length ) 
ws.delete_row(length)


# In[24]:


eleven_m_ago = pd.read_sql_query ('''
select distinct
prop.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(a.occurrence_date) as "Tgl Kunjungan", date(a.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when a.created_at is not null then 'ANC' end) as "Status",
concat(extract(month from a.created_at),'/',extract(year from a.created_at)) as bulan
from ancs a
	join reports on reports.reportable_id = a.id and reports.reportable_type = 'Sehati::Models::Anc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = a.healthcare_id
	join regions desa on desa.id = hv.region_id 
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions prop on prop.id = kota.parent_id
	join pregnancies p on p.id = a.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = a.midwife_id
where a.created_at + interval '1h'*desa.timezone >= date_trunc('month', CURRENT_DATE)
and a.occurrence_date < date(a.created_at)
and extract(month from a.occurrence_date) = extract(month from a.created_at) -11
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(p2.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(p2.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when p.created_at is not null then 'PNC' end) as "Status",
concat(extract(month from p2.created_at),'/',extract(year from p2.created_at)) as bulan
from pncs p2 
	join reports on reports.reportable_id = p2.id and reports.reportable_type = 'Sehati::Models::Pnc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = p2.healthcare_id
	join regions desa on desa.id = hv.region_id
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = p2.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = p2.midwife_id
where (p2.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and p2.filled_at < date(p2.created_at)
and extract(month from p2.filled_at + interval '1h'*desa.timezone) = extract(month from (p2.created_at + interval '1h'*desa.timezone)) -11
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(fo.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(fo.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when fo.created_at is not null then 'INC First Observation' end) as "Status",
concat(extract(month from fo.created_at),'/',extract(year from fo.created_at)) as bulan
from incs i
	join first_observations fo on fo.inc_id = i.id
	join reports on reports.reportable_id = i.id or reports.reportable_id = fo.id and reports.reportable_type = 'Sehati::Models::FirstObservation' or reports.reportable_type = 'Sehati::Models::Inc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = i.healthcare_id
	join regions desa on desa.id = hv.region_id
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = i.midwife_id
	where fo.filled_at is not null
 and (fo.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and fo.filled_at < date(fo.created_at)
and extract(month from (fo.filled_at + interval '1h'*desa.timezone)) = extract(month from (fo.created_at + interval '1h'*desa.timezone)) -11
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(o.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(o.created_at + interval '1h'*desa.timezone) as "Tanggal Dibuat",
(case when o.created_at is not null then 'INC Observation' end) as "Status",
concat(extract(month from o.created_at),'/',extract(year from o.created_at)) as bulan
from incs i
	join observations o on o.inc_id = i.id
	join reports on reports.reportable_id = i.id or reports.reportable_id = o.id and reports.reportable_type = 'Sehati::Models::Observation' or reports.reportable_type = 'Sehati::Models::Inc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = i.healthcare_id
	join regions desa on desa.id = hv.region_id and desa."level" = 3
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = o.midwife_id
 where (o.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and o.filled_at <date(o.created_at)
and extract(month from (o.filled_at + interval '1h'*desa.timezone)) = extract(month from (o.created_at + interval '1h'*desa.timezone)) -11
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
mi.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(m.maternity_time + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(m.created_at + interval '1h'*desa.timezone) as "Tanggal Dibuat",
(case when m.created_at is not null then 'INC Catatan Persalinian' end) as "Status",
concat(extract(month from m.created_at),'/',extract(year from m.created_at)) as bulan
from incs i
	join maternities m on m.inc_id = i.id
	join healthcares h on h.id = m.healthcare_id
	join regions desa on desa.id = m.region_id and desa."level" = 3
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users mi on mi.id = m.midwife_id
 where (m.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and m.maternity_time <date(m.created_at)
and extract(month from (m.maternity_time + interval '1h'*desa.timezone)) = extract(month from (m.created_at + interval '1h'*desa.timezone)) -11
order by 9,10
''',cnx)
eleven_m_ago


# In[25]:

ws = gc.open("Laporan Back Data Detail").worksheet("11M_ago")
dataframe = pd.DataFrame(ws.get_all_records())
length = (len(dataframe.index) + 2)
gd.set_with_dataframe(ws, eleven_m_ago,row = length ) 
ws.delete_row(length)


# In[26]:


twelve_m_ago = pd.read_sql_query ('''
select distinct
prop.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(a.occurrence_date) as "Tgl Kunjungan", date(a.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when a.created_at is not null then 'ANC' end) as "Status",
concat(extract(month from a.created_at),'/',extract(year from a.created_at)) as bulan
from ancs a
	join reports on reports.reportable_id = a.id and reports.reportable_type = 'Sehati::Models::Anc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = a.healthcare_id
	join regions desa on desa.id = hv.region_id 
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions prop on prop.id = kota.parent_id
	join pregnancies p on p.id = a.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = a.midwife_id
where a.created_at + interval '1h'*desa.timezone >= date_trunc('month', CURRENT_DATE)
and a.occurrence_date < date(a.created_at)
and extract(month from a.occurrence_date) = extract(month from a.created_at) -12
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(p2.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(p2.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when p.created_at is not null then 'PNC' end) as "Status",
concat(extract(month from p2.created_at),'/',extract(year from p2.created_at)) as bulan
from pncs p2 
	join reports on reports.reportable_id = p2.id and reports.reportable_type = 'Sehati::Models::Pnc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = p2.healthcare_id
	join regions desa on desa.id = hv.region_id
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = p2.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = p2.midwife_id
where (p2.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and p2.filled_at < date(p2.created_at)
and extract(month from p2.filled_at + interval '1h'*desa.timezone) = extract(month from (p2.created_at + interval '1h'*desa.timezone)) -12
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(fo.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(fo.created_at + interval '1h'*desa.timezone) as "Tgl Dibuat",
(case when fo.created_at is not null then 'INC First Observation' end) as "Status",
concat(extract(month from fo.created_at),'/',extract(year from fo.created_at)) as bulan
from incs i
	join first_observations fo on fo.inc_id = i.id
	join reports on reports.reportable_id = i.id or reports.reportable_id = fo.id and reports.reportable_type = 'Sehati::Models::FirstObservation' or reports.reportable_type = 'Sehati::Models::Inc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = i.healthcare_id
	join regions desa on desa.id = hv.region_id
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = i.midwife_id
	where fo.filled_at is not null
 and (fo.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and fo.filled_at < date(fo.created_at)
and extract(month from (fo.filled_at + interval '1h'*desa.timezone)) = extract(month from (fo.created_at + interval '1h'*desa.timezone)) -12
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
m.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(o.filled_at + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(o.created_at + interval '1h'*desa.timezone) as "Tanggal Dibuat",
(case when o.created_at is not null then 'INC Observation' end) as "Status",
concat(extract(month from o.created_at),'/',extract(year from o.created_at)) as bulan
from incs i
	join observations o on o.inc_id = i.id
	join reports on reports.reportable_id = i.id or reports.reportable_id = o.id and reports.reportable_type = 'Sehati::Models::Observation' or reports.reportable_type = 'Sehati::Models::Inc'
	join healthcare_villages hv on hv.region_id = reports.region_id
	join healthcares h on h.id = i.healthcare_id
	join regions desa on desa.id = hv.region_id and desa."level" = 3
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users m on m.id = o.midwife_id
 where (o.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and o.filled_at <date(o.created_at)
and extract(month from (o.filled_at + interval '1h'*desa.timezone)) = extract(month from (o.created_at + interval '1h'*desa.timezone)) -12
union
select distinct
propinsi.name as "Propinsi",
kota."name" as "Kota/Kabupaten",
kecamatan.name as "Kecamatan",
h.name as "Puskesmas",
desa."name" as "Desa",
mi.full_name as "Nama Bidan",
u.full_name as "Nama Ibu",
date(m.maternity_time + interval '1h'*desa.timezone) as "Tgl Kunjungan", date(m.created_at + interval '1h'*desa.timezone) as "Tanggal Dibuat",
(case when m.created_at is not null then 'INC Catatan Persalinian' end) as "Status",
concat(extract(month from m.created_at),'/',extract(year from m.created_at)) as bulan
from incs i
	join maternities m on m.inc_id = i.id
	join healthcares h on h.id = m.healthcare_id
	join regions desa on desa.id = m.region_id and desa."level" = 3
	join regions kecamatan on kecamatan.id = desa.parent_id
	join regions kota on kota.id = kecamatan.parent_id
	join regions propinsi on propinsi.id = kota.parent_id
	join pregnancies p on p.id = i.pregnancy_id
	join users u on u.id = p.mother_id
	join users mi on mi.id = m.midwife_id
 where (m.created_at + interval '1h'*desa.timezone) >= date_trunc('month', CURRENT_DATE)
and m.maternity_time <date(m.created_at)
and extract(month from (m.maternity_time + interval '1h'*desa.timezone)) = extract(month from (m.created_at + interval '1h'*desa.timezone)) -12
order by 9,10
''',cnx)
twelve_m_ago


# In[27]:

ws = gc.open("Laporan Back Data Detail").worksheet("12M_ago")
dataframe = pd.DataFrame(ws.get_all_records())
length = (len(dataframe.index) + 2)
gd.set_with_dataframe(ws, twelve_m_ago,row = length ) 
ws.delete_row(length)

# In[ ]:
today = datetime.date.today()
bulan = today.strftime("%m/%Y")

bulan
# In[ ]:
ws = gc.open("Laporan Back Data Detail").worksheet("Dashboard")
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
coln = ('N'+str(length))
col_a = ('A'+str(length)+')')
ws.append_row([bulan])
ws.update_acell(colb, formula+'(Current_M!$K:$K,'+col_a)
ws.update_acell(colc, formula+'(1M_ago!$K:$K,'+col_a)
ws.update_acell(cold, formula+'(2M_ago!$K:$K,'+col_a)
ws.update_acell(cole, formula+'(3M_ago!$K:$K,'+col_a)
ws.update_acell(colf, formula+'(4M_ago!$K:$K,'+col_a)
ws.update_acell(colg, formula+'(5M_ago!$K:$K,'+col_a)
ws.update_acell(colh, formula+'(6M_ago!$K:$K,'+col_a)
ws.update_acell(coli, formula+'(7M_ago!$K:$K,'+col_a)
ws.update_acell(colj, formula+'(8M_ago!$K:$K,'+col_a)
ws.update_acell(colk, formula+'(9M_ago!$K:$K,'+col_a)
ws.update_acell(coll, formula+'(10M_ago!$K:$K,'+col_a)
ws.update_acell(colm, formula+'(11M_ago!$K:$K,'+col_a)
ws.update_acell(coln, formula+'(12M_ago!$K:$K,'+col_a)



