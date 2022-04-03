# Oracle Responsys data integration into customers database

Generic code to interface Oracle responsys campaign management data into clients database. 

Various layouts that the generic code can handle is 

Email
https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsEmail.htm
SMS
https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsSMS.htm
Launch State
https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsLaunchState.htm 
MMS
https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsMMS.htm
Push
https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsPush.htm
MC
https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsMC.htm
Web Pus
https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsWebPush.htm
MVT
https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsMVT.htm
Holdout
https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsHoldout.htm
DMP
https://docs.oracle.com/en/cloud/saas/marketing/responsys-user/Help/CED/FileLayoutsDMP.htm


1) Responsys_table_production.py produces table structures based on the file layouts from above. Takes a parameter to produce tables for the type of file layout like email, SMS etc
2) Responsys_table_code_generator.py - produces insert script into staging table, duplicates tables and curated table
3) Responsys_merge_script.py - Generic interface code to handle various campain types and file types.
