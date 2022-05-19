"""
Purpose:Import multiple excel files to sql server
Created By: Gagan Nayyar
Date: 3rd March 2022
Version: 1.0.0
Updated on: --/--/----
"""

from plistlib import UID
import pandas as pd
import pyodbc
import os
import datetime
from shareplum import Site, Office365
from shareplum.site import Version
import json, os
from io import StringIO
import warnings
import time
warnings.filterwarnings("ignore")

#Varibles required for the script

USERNAME = 'XXXXX@email.com' #Sharpoint user name
PASSWORD = 'XXXXXXXXX' #sharepoint password
SHAREPOINT_URL = 'https://orgnisation_name.sharepoint.com' #sharepoint url
SHAREPOINT_SITE = 'https://orgnisation_name.sharepoint.com/sites/site' #Sharepoint Site
SHAREPOINT_DOC = 'Shared Documents'
FOLDER = 'Folder_name' #Folder name from where the data is required
UPLOAD_FOLDER = 'Updated_Folder' #Folder to move the uploaded file

#Varibles required for the script
#SQL Server name
server_name = "XXXXXXXXXXXX"
#Database Name
database_name = "database_name"
insert_query = """INSERT INTO nps (
name,
id,
salary
)
VALUES 
(
?,
?,
?)
"""
def auth():
    """Authenticate the sharepoint with credentials"""
    authcookie = Office365(SHAREPOINT_URL, username=USERNAME, password=PASSWORD).GetCookies()
    site = Site(SHAREPOINT_SITE, version=Version.v365, authcookie=authcookie)

    return site

def connect_folder(folder_name=FOLDER):
    """Connect to a particular folder in sharepoint"""
    auth_site = auth()

    sharepoint_dir = '/'.join([SHAREPOINT_DOC, folder_name])
    folder = auth_site.Folder(sharepoint_dir)

    return folder

def get_files_link_list():
    """Getting the files from a folder in sharepoint"""
    auth()
    files_list = []  # empty list for storing the pdf file links
    folder = connect_folder(folder_name=FOLDER)
    for i in folder.files:
        files_list.append(i['odata.id'][:-2])
    return files_list

def download_file(file_name, folder_name):
    """Download the file in a dataframe"""
    folder = connect_folder(folder_name)
    return folder.get_file(file_name)

def byte_object_to_df(df):
    """Convert byte object to dataframe"""
    s = str(df,'utf-8')
    data = StringIO(s)
    df1=pd.read_csv(data)
    return df1

def store_to_csv(df1, folder, name):
    df1.to_excel(folder+'/'+name+'.csv')
    print(f"File saved to location {folder}")

def get_file_from_path(folder):
    """Get all the file from a path"""
    try:
        list_of_files = os.listdir(folder) #getting list of files from the folder
    except Exception as e:
        return e
    with_path = []
    for i in list_of_files:       #Adding the folder to the list of files
        with_path.append(folder+'/'+i)
    return with_path

def import_files_to_sql_local(files_list, server_name, database_name, Driver='{SQL Server Native Client 11.0}'):
    """
    Pupose: Import multiple excel/csv files to sql server from a local folder
    Steps:
    1. Connect to SQL
    2. Iterate over a list of file and load each file in a dataframe
    3. Import the data to a sql server
    Note: In case you are import excel files
    please change the pd.read_csv to pd.read_excel
    """
    try:
        conn = pyodbc.connect(Driver=Driver,  #eastablish connection to sql server
                      Server=server_name,
                      Database=database_name,
                      trusted_connection='yes')
    except Exception as e:
        return e

    if files_list != []:

        for i in files_list:
            df = pd.read_excel(i) #Get the file in dataframe (Please ignore in case of sharepoint)
            df['Insert_Date'] = datetime.datetime.now() #Insert date coloum
            df['Source'] = i.split('\\')[-1] #The source file name coloum
            df = df.fillna(0) #filling nan with 0
            df = df.applymap(str) #Convert the dataframe in string
            cursor = conn.cursor() #create a cursor object
            cursor.fast_executemany = True #Enable fast execution
            cursor.executemany(insert_query, df.values.tolist())  # Main Execution
            cursor.commit() #Commit the cursor execution
            cursor.close()
            print(i)
        conn.close() #close over all connection
        return"Completed"
    else:
        return "No file to import"

def import_files_to_sql_sharepoint(files_list, server_name, database_name,  Driver='{SQL Server Native Client 11.0}'):
    """
    Pupose: Import multiple excel/csv files to sql server from a sharepoint folder
    Steps:
    1. Connect to SQL
    2. Iterate over a list of file and load each file in a dataframe
    3. Import the data to a sql server
    Note: In case you are import excel files
    please change the pd.read_csv to pd.read_excel
    """
    try:
        conn = pyodbc.connect(Driver=Driver,  #eastablish connection to sql server
                      Server=server_name,
                      Database=database_name,
                      trusted_connection='yes')
    except Exception as e:
        return e
    if files_list != []:
        for i in files_list:
            print(i)
            df = byte_object_to_df(download_file(file_name=i.split('/')[-1],folder_name=FOLDER))
            df['Insert_Date'] = datetime.datetime.now() #Insert date coloum
            df['Source'] = i.split('\\')[-1] #The source file name coloum
            df['ReferrerURL'] = " " #this is custom column. Please remove for your files
            df = df.fillna(0) #filling nan with 0
            df = df.applymap(str) #Convert the dataframe in string
            cursor = conn.cursor() #create a cursor object
            cursor.fast_executemany = True #Enable fast execution
            cursor.executemany(insert_query_nps, df.values.tolist())  # Main Execution
            cursor.commit() #Commit the cursor execution
            cursor.close()
            time.sleep(10)
           
        conn.close() #close over all connection
        return "Completed"
    else:
        return "No file to import"

def move_to(Folder, Upload_folder):
    """
    Move a file from one sharepoint folder to another.
    Parameters:
        Folder: The name of the folder from where the files
        have to move
        
        Upload_Folder: The name of the folder where the files 
        have to be moved
        
    Note: The files will be deleted from the "Folder" once
    they move to "Upload_Folder"
    """
    folder_move_from = connect_folder(folder_name=Folder)
    folder_move_to = connect_folder(folder_name=Upload_folder)
    for i in folder_move_from.files:
        df_byte = folder_move_from.get_file(i['Name'])
        time.sleep(2)
        folder_move_to.upload_file(df_byte,i['Name'])
        time.sleep(3)
        folder_move_from.delete_file(i['Name'])
    return "Files Moved"


def script_logs(script_name, file_names):
    import datetime as dt
    import time
    import sys

    """
    Logging success to script logs table in the database
    Params:
        script_name: str = The name of the scheduled script
        file_name_list: incase of any file
    """

    insert_query= "INSERT INTO script_logs(script_name, run_date, message, file_names) VALUES (?,?,?,?)"
    run_date = str(dt.datetime.now())
    message = "success"
    values = (script_name, run_date, message, str(file_names))

    conn = pyodbc.connect(Driver='{SQL Server Native Client 11.0}',  #eastablish connection to sql server
                      Server=server_name,
                      Database=database_name,
                      trusted_connection='yes')

    cursor = conn.cursor()
    cursor.execute(insert_query, values)
    conn.commit()
    print("Done")

    return conn.close()

#Running the code
files_list = get_files_link_list() #getting list of files
import_files_to_sql_sharepoint(files_list=files_list,
                               server_name=server_name,
                               database_name=database_name,
                               Driver='{SQL Server Native Client 11.0}'
                              )

script_logs("NPS to SQL", files_list)

move_to(Folder=FOLDER, Upload_folder=UPLOAD_FOLDER)
