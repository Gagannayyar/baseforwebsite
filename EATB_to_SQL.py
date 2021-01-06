import os
import pandas as pd
import numpy as np
import datetime
import time
import pyodbc
import calendar

start = time.time()

now = datetime.datetime.now()

week_name = input("Please enter the weekname in mm_dd_yy format or write EOM: ")

print(f"The process started at {now}")
folder_path = r"\\cernfs01\RevWx_BLR\BOS\Business Intelligence\Prodcutivity Reports\December_2020" + '\\' + week_name


def last_day_month(date):
    """Date in string format mm_dd_yy
    and get the last date in datetime format"""
    date = date.split("_")
    date[1] = str(1)
    mycal = calendar.monthcalendar(int(date[2]), int(date[0]))
    date[1] = str(max(max(mycal)))
    activity_date_1 = "-".join(date)
    last_date = datetime.datetime.strptime(activity_date_1, "%m-%d-%Y")
    return last_date


def weektoactivity(date):
    """Getting the activity date from the week_name input"""
    global activity_date
    date = date.split("_")
    if int(date[1]) + 7 < last_day_month("_".join(date)).day:
        date[1] = str(int(date[1]) + 7)
        activity_date_1 = "-".join(date)
        activity_date = datetime.datetime.strptime(activity_date_1, "%m-%d-%Y")
        return activity_date
    else:
        if int(date[0]) == 12:
            num = last_day_month("_".join(date)).weekday()
            date[0] = "01"
            date[2] = str(int(date[2]) + 1)
            date[1] = str(5 - num)
            activity_date_1 = "-".join(date)
            activity_date = datetime.datetime.strptime(activity_date_1, "%m-%d-%Y")
            print(activity_date)
            return activity_date
        else:
            num = last_day_month("_".join(date)).weekday()
            date[0] = str(int(date[0]) + 1)
            if num == 6:
                date[1] = "06"
                activity_date_1 = "-".join(date)
                activity_date = datetime.datetime.strptime(activity_date_1, "%m-%d-%Y")
                return activity_date
            if num == 5:
                date[1] = "07"
                activity_date_1 = "-".join(date)
                activity_date = datetime.datetime.strptime(activity_date_1, "%m-%d-%Y")
                return activity_date

            else:
                date[1] = str(5 - num)
                activity_date_1 = "-".join(date)
                activity_date = datetime.datetime.strptime(activity_date_1, "%m-%d-%Y")
                return activity_date


weektoactivity(week_name)
list_of_files = os.listdir(folder_path)

with_folder_files = []
for i in list_of_files:
    with_folder_file = os.path.join(folder_path, i)
    with_folder_files.append(with_folder_file)

all_data = pd.DataFrame()

for i in with_folder_files:
    df = pd.read_excel(i, sheet_name='Pivot')
    df.columns = df.loc[0]
    dff = df.copy()
    dff = dff.loc[1:]
    dff = dff.rename(columns={'DOS Aging Bucket': 'Status'})
    dff.reset_index(inplace=True, drop=True)
    dff['Final Status'] = None
    for j in range(1, len(dff[np.NaN])):
        dff['Final Status'][j] = dff[np.NaN][j]
    dff.drop(np.NaN, axis=1)
    dff = dff[1:]
    dff['Activity Date'] = activity_date
    dff['Client'] = (i.split(".")[0]).split("\\")[-1]
    dff.reset_index()
    all_data = all_data.append(dff, ignore_index=True)
    all_data = all_data.drop(np.NaN, axis=1)
print("All data uploaded to dataframe in python")

new_col_list = ['Final Status', 'Status', 'A. 0 to 30 days',
                'B. 31 to 60 days', 'C. 61 to 90 days', 'D. 91 to 120 days',
                'E. 121 to 150 days', 'F. 151 to 180 days', 'G. 181 to 365 days', 'H. 366 and above', 'Activity Date',
                'Client']

all_data = all_data[new_col_list]
all_data = all_data.reset_index()
all_data = all_data.drop('index', axis=1)
# all_data.fillna(0)
all_data = all_data.to_numpy()

""" 
Starting the SQL update using pyodbc library. The values in the tables are 
converted to numpy arrays so that the same can be appended using for loop into desired SQL Server
"""

# Connecting to SQL Server
server_name = "W1751904\LOCAL_CERNER"
database_name = "Test_Productivity_Dashboard"
conn = pyodbc.connect(Driver='{SQL Server Native Client 11.0}',
                      Server=server_name,
                      Database=database_name,
                      trusted_connection='yes')

cursor = conn.cursor()

# Uploading in SQL Action Code table
insert_query = """INSERT INTO EATBxtract ([Final Status]
      ,[Status]
      ,[0 to 30 days]
      ,[31 to 60 days]
      ,[61 to 90 days]
      ,[91 to 120 days]
      ,[121 to 150 days]
      ,[151 to 180 days]
      ,[181 to 365 days]
      ,[366 above]
      ,[Activity Date]
      ,[Client]) 
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """

for row in all_data:
    values = (row[0], row[1], row[2], row[3], row[4], row[5],
              row[6], row[7], row[8], row[9],
              row[10], row[11])
    cursor.execute(insert_query, values)

conn.commit()
print("EATB Pivot uploaded to SQL Server")

end = time.time()
print(f"The process completed at {round((end - start) / 60)} minutes")


