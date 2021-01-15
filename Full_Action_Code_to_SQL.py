# -*- coding: utf-8 -*-
"""
Created on Fri Jan 15 16:15:07 2021

@author: GN082282
"""


"""
This scripts is foruploading the action code report to SQL with select columns
"""

#Importing nessary libraries
import os
import datetime
import sys
import pandas as pd
import time
import numpy as np
import warnings
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")
from importlib import reload
import pyodbc
import pandas as pd

reload(sys)
pd.options.mode.chained_assignment = None

start = time.time()


print("The process has started at {}".format(datetime.datetime.now()))

#This is the main folderpath
folder = r'\\filesrvwhq\PowerWorks_Ops\Ambulatory Services\Client and Team Folders\0_New_Client_Folders'

#getting the right month folder
month_input = input("Please input month: ")
month_namefin = month_input.title()
month_name = month_input[0:3]
datetime_object = datetime.datetime.strptime(month_name, "%b")
month_number = datetime_object.month
folder_month = str(month_number)+'_'+str(month_namefin)
current_year = str(2020)
week_name = input("Please enter the week in mm_dd_yyyy format only or write EOM: ")
current_time = str(datetime.datetime.now())
#Getting the list of client folders\
list_of_c = [
 'Baptist_Health_BH_AL',
 'Barnabas_BARN_HS',
 'Benewah_BENE_ID',
 'Chinese_Hospital_CH_CA',
 'Nicklaus_Childrens_CHLD_FL',
 'Crozer_PHAN_PA',
 'Emory_Healthcare_EMCO_GA',
 'Escambia_ECHA_AL',
 'Humboldt_HUMB_NV',
 'Internal_Medicine_HSGN_LA',
 'Jackson_Parish_JPH_LA',
 'Lake_Health_LAKE_OH',
 'Land_O_Lakes_LNOL_MN',
 'Lawrence_LMH_KS',
 'Lawrence_LMH_KS_New_Domain',
 'Lexington_TCAH_NE',
 'LifePoint_LIFE_TN',
 'Maniilaq_MANQ_AK',
 'Simpson_General_SIMP_MS',
 'South_Florida_68906',
 'Torrance_TORR_CA',
 'Uintah_Basin_UBMC_UT',
 'Union_General_UNGH_GA',
 'Wray_Community_WCDH_CO']


list_of_folder = ['Baptist_Health_BH_AL',
 'Barnabas_BARN_HS',
 'Benewah_BENE_ID',
 'Chinese_Hospital_CH_CA',
 'Nicklaus_Childrens_CHLD_FL',
 'Crozer_PHAN_PA',
 'Emory_Healthcare_EMCO_GA',
 'Escambia_ECHA_AL',
 'Humboldt_HUMB_NV',
 'Internal_Medicine_HSGN_LA',
 'Jackson_Parish_JPH_LA',
 'Lake_Health_LAKE_OH',
 'Land_O_Lakes_LNOL_MN',
 'LifePoint_LIFE_TN',
 'Simpson_General_SIMP_MS',
 'South_Florida_68906',
 'Torrance_TORR_CA',
 'Uintah_Basin_UBMC_UT',
 'Union_General_UNGH_GA',
 'Wray_Community_WCDH_CO']

#Asking for local_path to save file
local_path = input("Please enter the path to save the file: ")


#Creating folder with path list
folderin_list = []
for i in list_of_folder:
    folderin = os.path.join(folder,i)
    folderin_foo = folderin + "\\" + current_year + "\\" + folder_month + "\\" + week_name
    folderin_list.append(folderin_foo)
      
#getting the EATB

if week_name =="EOM":
    file_name = "Action Codes EOM"
else:
    file_name = "Action Codes Weekly"

#Getting the list of interest and removing pseudo filesl
interest_list = []
for i in folderin_list:
    list_of_files = os.listdir(path=i)
    for j in list_of_files:
        if file_name in j:
            interest_list.append(i+'\\'+j)
            for m in interest_list:
                size = os.path.getsize(m)
                if size < 2000:
                    interest_list.remove(m)
                

#Copying the files
num = 0
all_data = pd.DataFrame()
for i in interest_list:
    df = pd.read_excel(i, sheet_name = "Action Code")
    df['Client'] = (i.split("- ")[2]).split(".")[0]
   
    all_data = all_data.append(df,ignore_index=True)
    print(f"Completed for {df['Client'][1]}")
    
total_time = time.time()


#Removing unnessary users
remove_users = ['Contributor_system , PARO',
'Contributor_system , PFS_COLLE', 
 'DO NOT MODIFY , THIS ACCOUNT',
 'DomainUser , Generated',
'Domainuser , Generated',
 'System , System',
'SYSTEM , SYSTEM']
name = " "
name_list = []
for i in all_data['Representative Name']:
    if i not in remove_users:
        name = i
        name_list.append(i)

all_data_filter = all_data[all_data['Representative Name'].isin(name_list)]
print("System generated rows removed")

#Changing the format of activity date and created date to MM-DD-YYYY
import datetime as dt
all_data_filter['Activity Date'] = pd.to_datetime(all_data_filter['Activity Date'].dt.strftime("%m-%d-%y"))
all_data_filter['Activity Date'] = all_data_filter['Activity Date'].apply(lambda x: x.date())
all_data_filter['Created Date'] = pd.to_datetime(all_data_filter['Created Date'].dt.strftime("%m-%d-%y"))
all_data_filter['Created Date'] = all_data_filter['Created Date'].apply(lambda x: x.date())



#Renaming the columns

all_data_semi = all_data_filter.rename(columns={'Encounter Number':'FIN', 
                                           'Organization': 'Organization Name',
                                            'Supervising Provider': 'Supervisor Name',
                                            'Transmission Date':'Last Claim Date'})

#Removing Columns

all_data_semi = all_data_semi.drop(['Generation Date', 'Submission Date'], axis=1)

#Converting currency to number

all_data_semi['Claim Amount'] =  all_data_semi['Claim Amount'].str.replace("$","")
all_data_semi['Encounter Balance'] =  all_data_semi['Encounter Balance'].str.replace("$","")
all_data_semi['Claim Amount'] =  all_data_semi['Claim Amount'].str.replace(",","")
all_data_semi['Encounter Balance'] =  all_data_semi['Encounter Balance'].str.replace(",","")
all_data_semi['Claim Amount'] =  all_data_semi['Claim Amount'].str.replace("(","")
all_data_semi['Encounter Balance'] = all_data_semi['Encounter Balance'].str.replace("(","")
all_data_semi['Claim Amount'] =  all_data_semi['Claim Amount'].str.replace(")","")
all_data_semi['Encounter Balance'] =  all_data_semi['Encounter Balance'].str.replace(")","")
all_data_semi['Claim Amount'].astype('float')
all_data_semi['Encounter Balance'].astype('float')

new_col_list = ['Activity Date', 'Billing Entity', 'FIN','Claim Number',
       'Health Plan', 'Discharge Date', 'Discharge Aging Range',
       'Last Claim Date', 'Claim Amount', 'Encounter Balance',
       'Supervisor Name', 'Representative Name', 'Action Code', 'Action Level',
       'Action Code Description',
       'Comment', 'Created Date','Client']

all_data_semi = all_data_semi[new_col_list]
print("Columns Updated")

all_data_semi.reset_index(drop=True)
all_data_fil = all_data_semi.to_numpy()


print("Action code uploaded to dataframe in python")

""" 
Starting the SQL update using pyodbc library. The values in the tables are 
converted to numpy arrays so that the same can be appended using for loop into desired SQL Server
"""

#Converting the NaT pandas values to None type
def remove_NaT():
    for i in all_data_fil:
        for n,j in enumerate(i):
            if j is pd.NaT:
                i[n] = None
                
                
    return all_data_fil



#Connecting to SQL Server
server_name = "W1751904\LOCAL_CERNER"
database_name = "Test_Productivity_Dashboard"
conn = pyodbc.connect(Driver='{SQL Server Native Client 11.0}',
                      Server=server_name,
                      Database=database_name,
                      trusted_connection='yes')


cursor = conn.cursor()


#Uploading in SQL Action Code table
insert_query = """INSERT INTO Action_Code ([Activity Date], [Billing Entity], [FIN],[Claim Number],
                                       [Health Plan], [Discharge Date], [Discharge Aging Range],
                                       [Last Claim Date], [Claim Amount], [Encounter Balance],
                                       [Supervisor Name], [Representative Name], [Action Code],
                                       [Action Level],[Action Code Description],[Comment],
                                       [Created Date],[Client]) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """


for row in all_data_fil:
    values = (row[0],str(row[1]),str(row[2]),str(row[3]),str(row[4]),row[5],str(row[6]),row[7],str(row[8]),row[9],str(row[10]),str(row[11]),str(row[12]),str(row[13]),str(row[14]),str(row[15]),row[16],str(row[17]))
    cursor.execute(insert_query,values)

conn.commit()
print("Action code uploaded to SQL Server")

#all_data_fil.to_excel(local_path+"\\"+list_of_folder[0]+".xlsx", index=False)
#print(f"File saved to {local_path}")

total_time = time.time()
print(f"The total time taken for the entire process was {round((total_time - start)/60)} minutes")