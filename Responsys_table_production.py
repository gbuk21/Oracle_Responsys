import xml.etree.ElementTree as ET
import requests
from bs4 import BeautifulSoup

import sys
param=sys.argv[1]

root = ET.Element("root")

if int(param)==1:
   url = 'https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsEmail.htm'
   output_file_name='email.csv'
   keyword="email"
elif int(param)==2:
   url='https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsSMS.htm'
   output_file_name='sms.csv'
   keyword="sms"
elif int(param)==3:
   url = 'https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsLaunchState.htm' 
   output_file_name='launch_state.csv'
   keyword="launch_state"
elif int(param)==4:
   url =  'https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsMMS.htm'
   output_file_name='mms.csv'  
   keyword="mms"
elif int(param)==5:
   url =  'https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsPush.htm'
   output_file_name='push.csv' 
   keyword="push"
elif int(param)==6:
   url =  'https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsMC.htm'
   output_file_name='message_center.csv' 
   keyword="message_center"
elif int(param)==7:
   url =  'https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsWebPush.htm'
   output_file_name='web_push.csv'    
   keyword="web_push"
elif int(param)==10:
   url =  'https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsMVT.htm'
   output_file_name='MVT.csv'  
   keyword="MVT"
elif int(param)==9:
   url =  'https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsHoldout.htm'
   output_file_name='holdout_group.csv'  
   keyword="holdout_group" 
elif int(param)==8:
   url =  'https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsDMP.htm'
   output_file_name='dmp.csv'   
   keyword="dmp"    
document = requests.get(url)
#document.content=document.content.replace("<br>","")

soup= BeautifulSoup(document.content,'html.parser')
head_count=0

file_to_write=open(output_file_name,'w')
row=''
tag_to_find=''
if int(param)>=1 and int(param)<=8:
   tag_to_find='h2'
else:
  tag_to_find='h1'
column_list=[]
column_list_row=[]

for docTitle in soup.find_all(tag_to_find):
    table_name=docTitle.text.replace(" ","_")
    row=table_name
    if int(param)>=1 and int(param)<=7: 
       siblling=docTitle.find_next_sibling()
    else:
       siblling=docTitle.find_next_sibling().find_next_sibling()
                
    head_count=head_count+1
    column_list=[]
    for trows in siblling.find_all('tr'):
        row=table_name
        #print(trows)
        column_list_row=[]
        p_no=0
        for throws in trows.find_all("td"):

            #print(throws)
            p_sibling1=throws.find_all("p")
            p_text=''

            for p_all in p_sibling1: 
                if 'CUSTOM_PROPERTIES' in p_all.text:
                   break
                #print(p_all.text)
                if p_no<3:
                   column_list_row.append(p_all.text.replace('<br>','').replace('\n',''))
                   #print(column_list_row)
                p_no=p_no+1
        column_list.append(column_list_row)
    
    #print(column_list)

    create_stage_sql=''
    create_curated_sql=''
    create_dup_sql=''
    create_errors_sql=''
    table = ET.SubElement(root, "table", name=table_name)

    col_loop=0
    create_stage_sql="create table responsys_"+str(keyword)+"_"+str(table_name)+"_stage ("
    for column_values in column_list:
        print(column_values)
        print(create_stage_sql)
        if len(column_values)>0:


           create_stage_sql=create_stage_sql+column_values[0]+" "+column_values[1]

           if column_values[2]=='NO':
              create_stage_sql=create_stage_sql+" NOT NULL"
           if col_loop<len(column_list)-3:
              create_stage_sql= create_stage_sql+","

           col_loop=col_loop+1
           
    create_stage_sql=create_stage_sql+")"
    
    col_loop=0
    create_curated_sql="create table responsys_"+str(keyword)+"_"+str(table_name)+" ("
    for column_values in column_list:
        #print(column_values)

        if len(column_values)>0:

           create_curated_sql=create_curated_sql+column_values[0]+" "+column_values[1]
           if column_values[2]=='NO':
              create_curated_sql=create_curated_sql+" NOT NULL"

           if col_loop<len(column_list)-3:
              create_curated_sql=create_curated_sql + ","
           col_loop=col_loop+1
           
    create_curated_sql=create_curated_sql+")"
 
    col_loop=0
    create_dup_sql="create table responsys_"+str(keyword)+"_"+str(table_name)+"_duplicate ("
    for column_values in column_list:
         #print(column_values)

         if len(column_values)>0:

            create_dup_sql=create_dup_sql+column_values[0]+" "+column_values[1]
            if column_values[2]=='NO':
               create_dup_sql=create_dup_sql+" NOT NULL"

            if col_loop<len(column_list)-3:
               create_dup_sql=create_dup_sql + ","
            col_loop=col_loop+1
            
    create_dup_sql=create_dup_sql+")"
    
    col_loop=0
    create_errors_sql="create table responsys_"+str(keyword)+"_"+str(table_name)+"_errors ("
    for column_values in column_list:
         
         #print(column_values)

         if len(column_values)>0: 
            create_errors_sql=create_errors_sql+column_values[0]+" "+column_values[1]
            if column_values[2]=='NO':
               create_errors_sql=create_errors_sql+" NOT NULL"

            if col_loop<len(column_list)-3:
               create_errors_sql=create_errors_sql  + "," 
            col_loop=col_loop+1

 
    create_errors_sql=create_errors_sql+")"    
    
    
    print(create_stage_sql)
    print(create_curated_sql)
    print(create_dup_sql)
    print(create_errors_sql)
    
    
    create_stage_sql_query = ET.SubElement(table, "create_stage_sql")
    create_stage_sql_query.text = create_stage_sql

    create_curated_sql_query = ET.SubElement(table, "create_curated_sql")
    create_curated_sql_query.text = create_curated_sql 

    create_dup_sql_query = ET.SubElement(table, "create_dup_sql")
    create_dup_sql_query.text = create_dup_sql 

    create_errors_sql_query = ET.SubElement(table, "create_errors_sql")
    create_errors_sql_query.text = create_errors_sql 
    
    tree = ET.ElementTree(root)
    tree.write(r"responsys_tables.xml") 
    
