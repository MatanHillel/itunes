import numpy as np
from numpy.random import randn
import pandas as pd
from pandas import Series, DataFrame
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
import glob as gb
import sqlalchemy as sql
from sqlalchemy import select
import psycopg2 as psy
import requests
from datetime import datetime, timedelta
import concurrent.futures



# reading the 3 json files:

data_path_1 = r'C:\Users\PC\Desktop\Pycharm_Projects\final_project_python_part\TXT_FILES\raw-department-budget.txt'
data_path_1_read = pd.read_json(data_path_1, lines = True)
df_converting_1 = pd.DataFrame(data_path_1_read)
# print(data_path_1_read)
########################################################
data_path_2 = r'C:\Users\PC\Desktop\Pycharm_Projects\final_project_python_part\TXT_FILES\raw-department-budget2.txt'
data_path_2_read = pd.read_json(data_path_2)
df_converting_2 = pd.DataFrame(data_path_2_read)
# print(data_path_2_read)
########################################################
data_path_3 = r'C:\Users\PC\Desktop\Pycharm_Projects\final_project_python_part\TXT_FILES\raw-department.txt'
data_path_3_read = pd.read_csv(data_path_3, delimiter='-', skiprows=1, names=['department_id', 'department_name'])
df_converting_3 = pd.DataFrame(data_path_3_read)
# print(data_path_3_read)

#####try_to_read_with_json!!!####
# data_path_3 = r'C:\Users\PC\Desktop\Pycharm_Projects\final_project_python_part\TXT_FILES\raw-department.txt'
# data_path_3_read = pd.read_json(data_path_3, lines = True, orient = 'columns')
# print(data_path_3_read)
########################################################


# ### calculates the budget for each department_id: ###
# # IT department total budget: (id_3)
# agg_IT = df_converting_2.groupby('department_id')['budget'].sum().reset_index()
# converting_to_df_2 = pd.DataFrame(agg_IT)
# # print(agg_IT)
#
# # sales support department total budget & General budget: (id_2) & (id_1)
# agg_sales_support = df_converting_1.groupby('department_id')['budget'].sum().reset_index()
# converting_to_df_1 = pd.DataFrame(agg_sales_support)
# # print(agg_sales_support)
#
# ##################################################################################
# ##################################################################################
#
# ### join the tables togather: ###
#
# # # join table 2 and 3:
# result_2_3 = pd.concat([agg_IT, agg_sales_support], ignore_index=True).reset_index()
# # print(result_2_3)
# result_2_3_sorted = result_2_3.sort_values(by = 'department_id').reset_index(drop = True)
# # print(result_2_3_sorted)
#
# # join the result to table 1:
# result_all_tables = pd.merge(data_path_3_read, result_2_3_sorted[['department_id', 'budget']], how='left', on='department_id')
# print(result_all_tables) # the table contains the 3 departments- ITm General, Sales Support and their budgets.


## way_2 ## :

merging_1 = df_converting_1.merge(df_converting_2, how='outer', on=['sub_dep_id', 'sub_dep_name', 'department_id', 'budget'])
# print(merging_1)

merging_2 = merging_1.merge(df_converting_3, how='left', on='department_id')
# print(merging_2)

Department_Table = merging_2.groupby(['department_id', 'department_name'])['budget'].sum().rename('Total Budget')
Department_Table = Department_Table.reset_index()
# print(Department_Table)


# Uploading to postgres in Dbeaver:
connection_string = 'postgresql://postgres:Admin@127.0.0.1/Chinook_Database'
db = sql.create_engine(connection_string)

with db.connect() as fff:  # fff or any other name
    Department_Table.to_sql('Department_Table', con = fff, if_exists = 'replace', index = False, schema = 'stg') #this adding to DATABASE in SQL




## API service part: ###

# 62ca1d800a84463488852cfd2c35b1e8
api_key = '62ca1d800a84463488852cfd2c35b1e8'  # Replace with your Open Exchange Rates API key
base_currency = 'USD'
target_currencies = ['ILS']  # Specify the target currencies, e.g., ILS, EUR, etc.

start_date = '2018-01-01'
end_date = '2022-12-22'  # Replace with your desired end date

date_format = '%Y-%m-%d'
start_datetime = datetime.strptime(start_date, date_format)
end_datetime = datetime.strptime(end_date, date_format)

def fetch_exchange_rate(current_date):
    url = f'https://openexchangerates.org/api/historical/{current_date}.json?base={base_currency}&symbols={",".join(target_currencies)}&app_id={api_key}'
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        exchange_rate = data['rates']['ILS']
        return {'Date': current_date, 'Exchange Rate': exchange_rate}
    else:
        print(f"Failed to fetch data for {current_date}")
        return None

exchange_rates_list = []

with concurrent.futures.ThreadPoolExecutor() as executor:
    date_range = [start_datetime + timedelta(days=i) for i in range((end_datetime - start_datetime).days + 1)]
    results = list(executor.map(fetch_exchange_rate, [date.strftime(date_format) for date in date_range]))

exchange_rates_list.extend(filter(None, results))

exchange_rates_df = pd.DataFrame(exchange_rates_list)

conn_string = 'postgresql://postgres:Admin@127.0.0.1/Chinook_Database'
db = sql.create_engine(conn_string)

with db.connect() as fff:  # fff or any other name
    exchange_rates_df.to_sql('Exchange_Table', con=fff, if_exists='replace', index=False, schema='stg') #this adding to DATABASE in SQL


##########################################
##########################################
##########################################
##########################################
##########################################
##########################################


from sqlalchemy import create_engine

# Set your database connection details
database_type = 'postgresql'  # Change this based on your database type
username = 'postgres'
password = 'Admin'
host = 'localhost'
port = '5432'
database_name = ''

# CSV file path
csv_file_path = 'path/to/your/file.csv'

# Read CSV file into a pandas DataFrame
df = pd.read_csv(csv_file_path)

# Create a database connection string
db_url = f'{database_type}://{username}:{password}@{host}:{port}/{database_name}'

# Create a database engine
engine = create_engine(db_url)

# Write the DataFrame to the database
df.to_sql('your_table_name', engine, index=False, if_exists='replace')

print("CSV data successfully inserted into the database.")
