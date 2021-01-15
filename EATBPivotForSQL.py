# -*- coding: utf-8 -*-
"""
Created on Fri Jan 15 18:41:02 2021

@author: GN082282
"""


# Importing nessary libraries
import os
import datetime
import sys
import pandas as pd
import time
import numpy as np
import os, copy, time, numpy as np
from openpyxl import load_workbook
from importlib import reload
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
if month_number > 9:
    folder_month = str(month_number)+'_'+str(month_namefin)
else:
    folder_month = "0"+str(month_number)+'_'+str(month_namefin)
    

current_year = str(2021)
week_name = input("Please enter the week in mm_dd_yyyy format only or write EOM: ")
current_time = str(datetime.datetime.now())
#Getting the list of client folders
list_of_folder = [
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

#Getting the month folder



#Asking for local_path to save file
local_path = r"\\cernfs01\RevWx_BLR\BOS\Business Intelligence\Prodcutivity Reports" + '\\'+ current_year + '\\' + folder_month + '\\' + week_name


#Creating folder with path list
folderin_list = []
for i in list_of_folder:
    folderin = os.path.join(folder,i)
    folderin_foo = folderin + "\\" + current_year + "\\" + folder_month + "\\" + week_name
    folderin_list.append(folderin_foo)
    
#Adding CHSI to folderin_list 
#chs = r"\\filesrvwhq\PowerWorks_Ops\Ambulatory Services\Client and Team Folders\0_New_Client_Folders\CHS_CHSI_TN"

#folderin_list.append(chs+'\\'+current_year+'\\'+folder_month+'\\'+week_name+'\\'+"CHSI2")
#folderin_list.append(chs+'\\'+current_year+'\\'+folder_month+'\\'+week_name+'\\'+"CHSI5")    

#getting the EATB

if week_name =="EOM":
    file_name = "EATB EOM"
else:
    file_name = "EATB Weekly"

#Getting the list of interest and removing pseudo files
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
                

#Starting the data manupulation
for i in interest_list:
    df_m = pd.read_excel(i, sheet_name = "Enhanced_ATB")
    df_m.rename(columns = {"Responsible Health Plan": "Current Health Plan", "Responsible Financial Class" : "Current Financial Class", "Balance Amount" : "Encounter Balance"}, inplace = True)
    print("Datatype of 'Encounter Number' in ATB - ", df_m['Encounter Number'].dtype)
    df = copy.deepcopy(df_m)
    df['Encounter Number'].fillna('Blanks', inplace=True)
    df['Encounter Number'] = df['Encounter Number'].astype(str)
    df['Balance Type 1'] = None
    df['Current Financial Class'].fillna((df['Primary Financial Class']), inplace=True)
    df['Balance Type 1'][df['Current Financial Class'].isin(['Self Pay', 'SELF PAY', 'Self Pay / Unknown', 'Self Pay Other Coverage Pending']) == True] = 'Self Pay'
    df['Balance Type 1'][df['Current Financial Class'].isin(['Self Pay', 'SELF PAY', 'Self Pay / Unknown', 'Self Pay Other Coverage Pending']) == False] = 'Insurance'
    print('\nUpdated - Balance Type')
    df['Category'] = None
    df['Category'][(df['Insurance Balance'] <= 0) & (df['Balance Type 1'] == 'Insurance')] = 'Credit Balance - Insurance'
    df['Category'][(df['Insurance Balance'] <= 0) & (df['Balance Type 1'] == 'Self Pay')] = 'Credit Balance - Self Pay'
    df['Category'][(df['Discharge Aging Category'] == 'DNFB') & (df['Balance Type 1'] == 'Insurance') & df['Category'].isnull()] = 'DNFB - Insurance'
    df['Category'][(df['Discharge Aging Category'] == 'DNFB') & (df['Balance Type 1'] == 'Self Pay') & df['Category'].isnull()] = 'DNFB - Self Pay'
    df['Category'][(df['Discharge Aging Category'] == 'Not Aged') & (df['Balance Type 1'] == 'Insurance') & df['Category'].isnull()] = 'Not Aged - Insurance'
    df['Category'][(df['Discharge Aging Category'] == 'Not Aged') & (df['Balance Type 1'] == 'Self Pay') & df['Category'].isnull()] = 'Not Aged - Self Pay'
    df['Category'][(df['Balance Type 1'] == 'Self Pay') & df['Category'].isnull()] = 'Self Pay'
    df['Category'][(df['Balance Type 1'] == 'Insurance') & df['Category'].isnull()] = 'Insurance'
    print('\nUpdated - Category')
    df['FBD Age'] = None
    df['FBD Age'] = (pd.to_datetime((df['Activity Date']), format='%m/%d/%Y') - pd.to_datetime((df['First Claim Submission Date']), format='%m/%d/%Y')).dt.days
    print('\nUpdated - First billed date age')
    df['FBD Aging'] = None
    df['FBD Aging'][df['FBD Age'].isin(list(range(0, 31)))] = '0-30'
    df['FBD Aging'][df['FBD Age'].isin(list(range(31, 91)))] = '31-90'
    df['FBD Aging'][df['FBD Age'].isin(list(range(91, 181)))] = '91-180'
    df['FBD Aging'][df['FBD Age'].isin(list(range(181, 366)))] = '181-365'
    df['FBD Aging'][df['FBD Age'].isin(list(range(366, 43784)))] = '366+'
    print('\nUpdated - First billed date aging bucket')
    df['LBD Age'] = None
    df['LBD Age'] = (pd.to_datetime((df['Activity Date']), format='%m/%d/%Y') - pd.to_datetime((df['Last Claim Transmission Date']), format='%m/%d/%Y')).dt.days
    print('\nUpdated - Last billed date age')
    df['LBD Aging'] = None
    df['LBD Aging'][df['LBD Age'].isin(list(range(0, 31)))] = '0-30'
    df['LBD Aging'][df['LBD Age'].isin(list(range(31, 91)))] = '31-90'
    df['LBD Aging'][df['LBD Age'].isin(list(range(91, 181)))] = '91-180'
    df['LBD Aging'][df['LBD Age'].isin(list(range(181, 366)))] = '181-365'
    df['LBD Aging'][df['LBD Age'].isin(list(range(366, 43784)))] = '366+'
    print('\nUpdated - Last billed date aging bucket')
    df['DOS Age'] = None
    df['DOS Age'] = (pd.to_datetime((df['Activity Date']), format='%m/%d/%Y') - pd.to_datetime((df['Discharge Date']), format='%m/%d/%Y')).dt.days
    df['DOS Age'][df['Discharge Date'].isnull()] = 0
    df['DOS Aging Bucket'] = None
    df['DOS Aging Bucket'][(df['DOS Age'] >= 0) & (df['DOS Age'] < 31)] = 'A. 0 to 30 days'
    df['DOS Aging Bucket'][(df['DOS Age'] >= 31) & (df['DOS Age'] < 61)] = 'B. 31 to 60 days'
    df['DOS Aging Bucket'][(df['DOS Age'] >= 61) & (df['DOS Age'] < 91)] = 'C. 61 to 90 days'
    df['DOS Aging Bucket'][(df['DOS Age'] >= 91) & (df['DOS Age'] < 121)] = 'D. 91 to 120 days'
    df['DOS Aging Bucket'][(df['DOS Age'] >= 121) & (df['DOS Age'] < 151)] = 'E. 121 to 150 days'
    df['DOS Aging Bucket'][(df['DOS Age'] >= 151) & (df['DOS Age'] < 181)] = 'F. 151 to 180 days'
    df['DOS Aging Bucket'][(df['DOS Age'] >= 181) & (df['DOS Age'] < 366)] = 'G. 181 to 365 days'
    df['DOS Aging Bucket'][df['DOS Age'] >= 366] = 'H. 366 and above'
    df['DOS Aging Bucket'].replace('', 'A. 0 to 30 days', inplace=True)
    print('\nUpdated - DOS Aging Bucket')
    df['Worked in last 15 days'] = None
    df['Worked in last 15 days'] = (pd.to_datetime((df['Activity Date']), format='%m/%d/%Y') - pd.to_datetime((df['Encounter Last Touch Date']), format='%m/%d/%Y')).dt.days
    df['Worked in last 15 days Bucket'] = None
    df['Worked in last 15 days Bucket'][(df['Worked in last 15 days'] >= 0) & (df['Worked in last 15 days'] < 16)] = 'A. 0 to 15 days'
    df['Worked in last 15 days Bucket'][(df['Worked in last 15 days'] >= 16) & (df['Worked in last 15 days'] < 31)] = 'B. 15 to 30 days'
    df['Worked in last 15 days Bucket'][(df['Worked in last 15 days'] >= 31) & (df['Worked in last 15 days'] < 61)] = 'C. 31 to 60 days'
    df['Worked in last 15 days Bucket'][(df['Worked in last 15 days'] >= 61) & (df['Worked in last 15 days'] < 91)] = 'D. 61 to 90 days'
    df['Worked in last 15 days Bucket'][(df['Worked in last 15 days'] >= 91) & (df['Worked in last 15 days'] < 121)] = 'E. 91 to 120 days'
    df['Worked in last 15 days Bucket'][(df['Worked in last 15 days'] >= 121) & (df['Worked in last 15 days'] < 151)] = 'F. 121 to 150 days'
    df['Worked in last 15 days Bucket'][(df['Worked in last 15 days'] >= 151) & (df['Worked in last 15 days'] < 181)] = 'G. 151 to 180 days'
    df['Worked in last 15 days Bucket'][(df['Worked in last 15 days'] >= 181) & (df['Worked in last 15 days'] < 366)] = 'H. 181 to 365 days'
    df['Worked in last 15 days Bucket'][df['Worked in last 15 days'] >= 366] = 'F. 366 and above'
    df['Worked in last 15 days Bucket'].replace('', 'A. 0 to 30 days', inplace=True)
    print('\nUpdated - Worked in last 15 days Bucket')
    df['Final Status'] = None
    df['Final Status'][(df['Insurance Balance'] <= 0) & (df['Discharge Aging Category'].isin(['DNFB', 'Not Aged']) == False)] = 'Self Pay/Zero Balance'
    df['Final Status'][df['Discharge Aging Category'].isin(['DNFB', 'Not Aged']) & df['Final Status'].isnull()] = 'Not-Factored'
    df['Final Status'][(df['FBD Aging'] == '0-30') & df['Final Status'].isnull()] = 'FBD 0-30'
    df['Final Status'][(df['LBD Aging'] == '0-30') & df['Final Status'].isnull()] = 'LBD 0-30'
    df['Final Status'][(df['DOS Aging Bucket'] == 'A. 0 to 30 days') & df['Final Status'].isnull()] = 'Discharge 0-30'
    df['Final Status'][df['Last Denial Reason'].notnull() & df['Final Status'].isnull()] = 'Technical Denial'
    df['Final Status'][(df['Worked in last 15 days Bucket'] == 'A. 0 to 15 days') & df['Final Status'].isnull()] = 'Worked in last 15 days'
    df['Final Status'][df['Final Status'].isnull()] = 'Workable'
    print('\nUpdated - Final Status')
    df['Status'] = None
    df['Status'][df['Final Status'] == 'Self Pay/Zero Balance'] = 'Not-Factored'
    df['Status'][df['Final Status'] == 'Not-Factored'] = 'Not-Factored'
    df['Status'][df['Final Status'] == 'FBD 0-30'] = 'Non-Workable'
    df['Status'][df['Final Status'] == 'LBD 0-30'] = 'Non-Workable'
    df['Status'][df['Final Status'] == 'Discharge 0-30'] = 'Non-Workable'
    df['Status'][df['Final Status'] == 'Worked in last 15 days'] = 'Non-Workable'
    df['Status'][df['Final Status'] == 'Technical Denial'] = 'Workable'
    df['Status'][df['Final Status'] == 'Workable'] = 'Workable'
    print('\nUpdated - Status')
    print('\nCreating the pivot table...')
    df_t = pd.pivot_table(df, index=['Final Status', 'Status'], columns=['DOS Aging Bucket'], values=['Encounter Number'], aggfunc='count', fill_value=0)
    print('\nCurrent ATB_Pivot has been created')
    print('\nReport generated! Saving the file...')
    dfn = pd.ExcelWriter(local_path + '\\' + i.split("- ")[-1])
    #df.to_excel(dfn, sheet_name='Worked report', encoding='cp1251', index=False)
    df_t.to_excel(dfn, sheet_name='Pivot', encoding='cp1251')
    dfn.save()
    dfn.close()
    print('\nWorked report has been saved.\nPlease wait...')
    end = time.time()
    time_taken = (end - start) / 60
    print('\nTime taken - ', round(time_taken, 2), 'minutes')
    print("{} is updated now and ready to use".format(i.split("- ")[-1]))

total_time = (time.time() - start)/60
print("All the productivity files are updated now")
print(f"Total time taken by program to run is {round(total_time,2)} minutes")