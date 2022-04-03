import xml.etree.ElementTree as ET
import requests,glob
from bs4 import BeautifulSoup
import os,sqlite3
import pandas as pd
import numpy as np
from sqlalchemy.types import Integer, String
from sqlalchemy.dialects.oracle import NUMBER, VARCHAR2, DATE
from sqlalchemy import types, create_engine
from sqlalchemy.exc import SQLAlchemyError

url = 'responsys.xml'

with open(url, 'r') as f:
    data = f.read()

soup= BeautifulSoup(data,"lxml")

conn = create_engine('sqlite:////path_to_db/responsys.db')

import sys
param=sys.argv[1]

if int(param)==1:
   table_name='Sent'
elif int(param)==2:
   url='https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsSMS.htm'
   output_file_name='sms.csv'
elif int(param)==3:
   url = 'https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsLaunchState.htm'
   output_file_name='launch_state.csv'
elif int(param)==4:
   url =  'https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsMMS.htm'
   output_file_name='mms.csv'

elif int(param)==5:
   url =  'https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsPush.htm'
   output_file_name='push.csv'

elif int(param)==6:
   url =  'https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsMC.htm'
   output_file_name='message_center.csv'

elif int(param)==7:
   url =  'https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsWebPush.htm'
   output_file_name='web_push.csv'

elif int(param)==10:
   url =  'https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsMVT.htm'
   output_file_name='MVT.csv'

elif int(param)==9:
   url =  'https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsHoldout.htm'
   output_file_name='holdout_group.csv'

elif int(param)==8:
   url =  'https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsDMP.htm'
   output_file_name='dmp.csv'

print(soup)
b_name = soup.find('table', {'name':table_name})
print(b_name)

for docTitle in soup.find_all('insert_into_stg'):
    if table_name in docTitle.text:
       insert_into_stg_query=docTitle.text
merge_into_query=''
for docTitle in soup.find_all('merge_into'):
    if table_name in docTitle.text:
       merge_into_query=docTitle.text
insert_into_dup_query=''
for docTitle in soup.find_all('insert_into_dup'):
    if table_name in docTitle.text:
       insert_into_dup_query=docTitle.text
print(insert_into_dup_query)
print(merge_into_query)
#print(insert_into_stg_query)

df_to_write=pd.DataFrame(data=None, index=None, columns=None, dtype=None, copy=False)
for filename in glob.glob('/path_to_file/account_no_SENT_20220327_070014.txt.gz'):
    for df in pd.read_csv(filename, compression='gzip', sep='|',chunksize=1000000,na_values=np.NaN ):
        print(df.head(5))
        print(df.dtypes)
        stage_table='responsys_email_'+table_name+'_stage'
        #try:
        if 1==1:
            df.to_sql(name = stage_table,con= conn, index=False, if_exists='append')
        #except SQLAlchemyError:
        #    print("Error occurred during inserting into staging table")
        #    sys.exit(1)

curated_table_creation="create table responsys_email_"+table_name+" as select * from responsys_email_"+table_name+"_stage where 1=2"
try:
    conn.execute(curated_table_creation)
except:
    pass
duplicates_table_creation="create table responsys_email_"+table_name+"_duplicate as select * from responsys_email_"+table_name+"_stage where 1=2"
try:
   conn.execute(duplicates_table_creation)
except:
   pass

try:
       conn.execute(insert_into_dup_query)
except SQLAlchemyError:
            print("Error occurred during inserting into duplicates table")
            sys.exit(1)

try:
       conn.execute(merge_into_query)
except SQLAlchemyError:
            print("Error occurred during merge")
            sys.exit(1)

#conn.commit()
