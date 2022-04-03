import xml.etree.ElementTree as ET
import requests
from bs4 import BeautifulSoup
import os

root = ET.Element("root")


import sys
param=sys.argv[1]

key_columns=['Event_Type_ID','Account_ID','RIID','Event_Captured_Dt','Campaign_ID','Launch_ID']

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
                   column_list_row.append(p_all.text.replace('<br>','').replace('\\n','').replace('\n',''))
                   #print(column_list_row)
                p_no=p_no+1
        column_list.append(column_list_row)
    
    #print(column_list)
   
    #print("df=pd.read_csv(filename, sep='|')")
   

    #print("big_ind_list=df_to_write.values.tolist()")
    insert_into_stg='INSERT INTO Responsys_'+keyword+'_'+table_name+'_stage(' 
    select_from_stg=' from Responsys_'+keyword+'_'+table_name+'_stage' 
    merge_into='MERGE INTO Responsys_'+keyword+'_'+table_name+' a using (' 
    merge_select='select '
    bind_variables=' ('
    on_clause= " on (" 
    
    merge_insert_clause=' WHEN NOT MATCHED THEN insert ('
    merge_insert_values_clause=' values ('
    
    #insert_into_dup='INSERT INTO Responsys_'+keyword+'_'+table_name+'_duplicate	select ' 
    insert_into_dup='INSERT INTO Responsys_'+keyword+'_'+table_name+'_duplicate ' 
    insert_into_curated='INSERT INTO Responsys_'+keyword+'_'+table_name+' '
    insert_into_dup_cols=''
    insert_into_select_cols=''
    insert_into_exists_cols=''
    
    col_loop=0
    for column_values in column_list:
           if len(column_values)>0:
               #print(column_values[0].strip())
               for key_column in key_columns:
                  if column_values[0].strip()  ==key_column.upper():                  
                     on_clause=on_clause+" a."+column_values[0]+"="+" b."+column_values[0]
                     on_clause=on_clause+ " and" 
                     insert_into_exists_cols=insert_into_exists_cols+" a."+column_values[0]+"="+" b."+column_values[0]
                     insert_into_exists_cols=insert_into_exists_cols+ " and" 
                     
               insert_into_stg=insert_into_stg+column_values[0] 
               if col_loop<len(column_list)-2:
                  bind_variables=bind_variables+":"+str(col_loop+1)   
               merge_select=merge_select+column_values[0]
               merge_insert_clause=merge_insert_clause+column_values[0] 
               merge_insert_values_clause=merge_insert_values_clause+"b."+column_values[0]
               insert_into_select_cols=insert_into_select_cols+column_values[0] 
               
               #print(col_loop)
               #print(len(column_list))
               if col_loop<len(column_list)-3:
                  insert_into_stg=insert_into_stg  + "," 
                  bind_variables=bind_variables+ ","
                  merge_select=merge_select+ ","   
                  merge_insert_clause=merge_insert_clause+ ","   
                  merge_insert_values_clause=merge_insert_values_clause+ "," 
                  insert_into_select_cols=insert_into_select_cols+ "," 
                  
               elif col_loop<len(column_list)-2:
                    insert_into_stg=insert_into_stg  + ")" 
                    bind_variables=bind_variables+ ")"   
                    merge_select=merge_select+ select_from_stg+") b" 
                    on_clause=on_clause+")"
                    insert_into_exists_cols=insert_into_exists_cols+")"
                    merge_insert_clause=merge_insert_clause+")"
                    merge_insert_values_clause=merge_insert_values_clause+")"
               col_loop=col_loop+1
    on_clause_split=on_clause.split(' ')
    insert_into_exists_cols_split=insert_into_exists_cols.split(' ')
    print(on_clause_split)
    print(on_clause_split[-1])
    if on_clause_split[-1]=='and)':
       on_clause_meged=' '.join(on_clause_split[0:-1])
       on_clause_meged=on_clause_meged+')'
    else:
       on_clause_meged=on_clause

    if insert_into_exists_cols_split[-1]=='and)':
       insert_exists_meged=' '.join(insert_into_exists_cols_split[0:-1])
       insert_exists_meged=insert_exists_meged+')'
    else:
       insert_exists_meged=insert_into_exists_cols
       
    insert_into_stg=insert_into_stg+" values " + bind_variables
    merge_into=merge_into+merge_select+on_clause_meged+merge_insert_clause+merge_insert_values_clause
    
    insert_into_dup=insert_into_dup+"("+insert_into_select_cols[:-1]+") select " + insert_into_select_cols[:-1] +" " +select_from_stg + " a where exists ("+" select 1 from Responsys_"+keyword+'_'+table_name+" b where  "+insert_exists_meged
    insert_into_curated=insert_into_curated+"("+insert_into_select_cols[:-1]+") select " + insert_into_select_cols[:-1] +" " +select_from_stg + " a where exists ("+" select 1 from Responsys_"+keyword+'_'+table_name+" b where  "+insert_exists_meged    

    table = ET.SubElement(root, "table", name=table_name+'_'+keyword)
 
    # Add query
    insert_into_stg_query = ET.SubElement(table, "insert_into_stg")
    insert_into_stg_query.text = insert_into_stg

    merge_into_query = ET.SubElement(table, "merge_into")
    merge_into_query.text = insert_into_curated 

    insert_into_dup_query = ET.SubElement(table, "insert_into_dup")
    insert_into_dup_query.text = insert_into_dup 

    tree = ET.ElementTree(root)
    tree.write(r"c://temp4//responsys.xml") 
    
    #print(insert_into_stg)
    #print('\n')
    #print(merge_into)
    #print('\n')
    #print(insert_into_dup)
    
    #insert_into_stg=insert_into_stg+
    #print('sqlite_insert_query = "INSERT INTO tab_file_key_value_index( "+append_cols+"table_file_index, row_index) VALUES ( "+append_bind+"?,?);"
    #print(sqlite_insert_query)
    #c1.executemany(sqlite_insert_query, big_ind_list)
    
    
    
    
