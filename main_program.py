# -*- coding: utf-8 -*-
"""
@author: Pipatphon Lapamonpinyo
Web: https://csun.uic.edu/codes/PAWU.html
"""
#==============================================================================
#                           *** Main Program ***
#==============================================================================
#import essencial modules
from create_db_and_table_03 import *
from create_db_and_table_03 import load_myvars_configurations
from create_db_and_table_03 import create_mydb
from create_db_and_table_03 import create_table

#******************************************************************************
#                  Part I Create Empty database and Tables
#******************************************************************************
#------------------------------------------------------------------------------
#1) Read variables from ".yaml" file
print("Step1:")
yaml_filename = "amtrak_vars.yaml"    
print("Loading pre-defined variables from \""+ yaml_filename+ "\"...")

#function -> load_myvars_configurations(yaml_filename)
myvars = load_myvars_configurations(yaml_filename)

#------------------------------------------------------------------------------
#2) Create a new database
print("Step2:")
#2.1 Assign variables' values from the loaded *.yaml above for creating database
create_mydb(myvars["db_admin"])

#------------------------------------------------------------------------------   
#3) Create stations, departure, arrival, and weather tables
print("Step3.1:")
#Function format: create_table(my_host, my_user, my_passwd, my_db, my_table_name, field_list, pk, fk)
#3.1 Create "stations" table
create_table(myvars["db_admin"], myvars["db_admin"]["stations_table_name"], myvars['stations_table'], myvars['stations_pk'], myvars['stations_fk'] )

#3.2 Create "departure" table (id,name,lat,long,city,etc.)
create_table(myvars["db_admin"], myvars["db_admin"]["departure_table_name"], myvars['departure_table'], myvars['departure_pk'], myvars['departure_fk'])

#3.3 Create arrival table
create_table(myvars["db_admin"], myvars["db_admin"]["arrival_table_name"], myvars['arrival_table'], myvars['arrival_pk'], myvars['arrival_fk'])

#3.4 Create weather table
create_table(myvars["db_admin"], myvars["db_admin"]["weather_table_name"], myvars['weather_table'], myvars['weather_pk'], myvars['weather_fk'])

#------------------------------------------------------------------------------
#3) Create ridership_total
print("Step3.2:")
create_table(myvars["db_admin"], myvars["db_admin"]["ridership_total_table_name"], myvars['ridership_total_table'], myvars['ridership_total_pk'], myvars['ridership_total_fk'] )

#******************************************************************************
#                  Part II: Retrive Data to MySql database 
#******************************************************************************

#==============================================================================
#4) Retrive stations data from Amtack(*.csv file) to MySql
print("Step4:")
#4.1 Load amtack stations from *.CSV file to datafram before saving it to MySql
import pandas as pd
csv_file_name =  myvars['stations_csv_filename']
print("Reading stations data from csv file, \""+csv_file_name+"\"")
data = pd.read_csv(csv_file_name, header=0, index_col=False)

#Print number of rows and colums read
print("{0} rows and {1} columns".format(len(data.index), len(data.columns)))
print("")

#4.2 Retrive data from *.csv file to MySql
#Sqlalchemy to store Dataframe to mySql
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
db_user = myvars["db_admin"]["user"]
db_passwd = myvars["db_admin"]["passwd"]
db_host = myvars["db_admin"]["host"]
db_name = myvars["db_admin"]["db_name"]
table_name = myvars["db_admin"]["stations_table_name"]
#format: mysql+mysqlconnector://<user>:<password>@<host>[:<port>]/<dbname>
engine = sqlalchemy.create_engine('mysql+pymysql://'+str(db_user)+':'+str(db_passwd)+'@'+str(db_host)+':3306/'+str(db_name))

try:
    data.to_sql(name=str(table_name), con=engine, if_exists = 'append', index=False)
    print("Successfully loaded stations data from csv file to MySQL!\n")

except SQLAlchemyError as e:
    print(e)

#==============================================================================    
#------------------------------------------------------------------------------
#5) Retrive stations data from Amtack to MySQL - url address is needed
#Loading pre-definded train_no, start_date, end_date
from create_db_and_table_03 import load_myvars_configurations
yaml_filename2 = "amtrak_vars2.yaml"    
print("Loading pre-defined variables from \""+ yaml_filename2+ "\"...")

#function -> load_myvars_configurations(yaml_filename)
myvars2 = load_myvars_configurations(yaml_filename2)
print("Step5:")

#Station List for retrieving data from Amtack and WU
#station_list = ["CHI", "GLN", "SVT", "MKA", "MKE"]
station_list = myvars2["station_list"]
station_list_for_searching = myvars2["station_list_for_searching"]
train_number_list = myvars2["train_number_list"]

from load_amtrack_dep_arr_time_to_my_sql_002 import *
from load_amtrack_dep_arr_time_to_my_sql_002 import get_departure_from_amtrak_to_dp_mysql
from load_amtrack_dep_arr_time_to_my_sql_002 import get_arrival_from_amtrak_to_dp_mysql
from load_amtrack_dep_arr_time_to_my_sql_002 import get_departure_from_amtrak_to_dp_mysql_stncode
from load_amtrack_dep_arr_time_to_my_sql_002 import get_arrival_from_amtrak_to_dp_mysql_stncode
#d_start = '01/25/2008'
start_date = '/'.join([str(myvars2["start_date"]["month"]).zfill(2), str(myvars2["start_date"]["day"]).zfill(2), str(myvars2["start_date"]["year"])])
end_date = '/'.join([str(myvars2["end_date"]["month"]).zfill(2), str(myvars2["end_date"]["day"]).zfill(2), str(myvars2["end_date"]["year"])])

#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#Option = 1: searching ny train number, option = 2: searching by station code 
if (myvars2["searching_option"] == 1):
    #Have to clear station list first***
    from update_basic_yaml_file import clear_stationlist_in_basic_yaml  
    clear_stationlist_in_basic_yaml(yaml_filename2)
    
    for train_num in train_number_list:
        #set flag for first year = True
        start_date = '/'.join([str(myvars2["start_date"]["month"]).zfill(2), str(myvars2["start_date"]["day"]).zfill(2), str(myvars2["start_date"]["year"])])
        end_date = '/'.join([str(myvars2["end_date"]["month"]).zfill(2), str(myvars2["end_date"]["day"]).zfill(2), str(myvars2["end_date"]["year"])])

        for while_year in range(myvars2["start_date"]["year"], myvars2["end_date"]["year"]+1):
            print(while_year)
            #Set start_date and end_date for each batch of retreiving departure and arrival data from Amtrak
            if (while_year == myvars2["start_date"]["year"]):
                #start_date = original start_date
                start_date = '/'.join([str(myvars2["start_date"]["month"]).zfill(2), str(myvars2["start_date"]["day"]).zfill(2), str(myvars2["start_date"]["year"])])
                #end_date = 31/12/start_year
                end_date = '/'.join(['12', '31', str(myvars2["start_date"]["year"])])
            elif(while_year == myvars2["end_date"]["year"]):
                start_date = '/'.join(['01', '01', str(while_year)])
                end_date = '/'.join([str(myvars2["end_date"]["month"]).zfill(2), str(myvars2["end_date"]["day"]).zfill(2), str(myvars2["end_date"]["year"])])
            else:
                start_date = '/'.join(['01', '01', str(while_year)])
                end_date = '/'.join(['12', '31', str(while_year)])
            
            print(start_date)
            print(end_date)
            #5.1 Departure
            get_departure_from_amtrak_to_dp_mysql(myvars["db_admin"], train_num, start_date, end_date)
            #5.2 Arrival
            get_arrival_from_amtrak_to_dp_mysql(myvars["db_admin"], train_num, start_date, end_date)

        #==========================================================================
        #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        #Add code to automatically get station list here for searching ny train number specifically
        from get_departure_arrival_from_mySQL import get_departure_data_from_mysql_by_trainnumber
        from get_departure_arrival_from_mySQL import get_arrival_data_from_mysql_by_trainnumber
        
        df_departure = get_departure_data_from_mysql_by_trainnumber(myvars["db_admin"], train_num)
        df_arrival = get_arrival_data_from_mysql_by_trainnumber(myvars["db_admin"], train_num)
        
        #Filter unique stations from departure table and merge with  unique stations from srrival table
        unique_stations_id_list = list(df_departure.STN_ID.unique())
        unique_stations_id_list = list(set(unique_stations_id_list + list(df_arrival.STN_ID.unique())))
        
        #Convert STN_ID to STN_CODE
        from amtack_stations_02 import get_stncode_by_stnid
        #get_stncode_by_stnid(db_admin, STN_ID):
        unique_stations_code_list = list([])
        for a_station in unique_stations_id_list:
            temp = get_stncode_by_stnid(myvars["db_admin"], int(a_station))
            unique_stations_code_list.append(temp)
        
        #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        #Station list is automatically formed above, but needs to be assigned as follow
        station_list = unique_stations_code_list
        
        #Update/add station list in *yaml file
        # Call function *******************************************************
        from update_basic_yaml_file import add_a_station_code_list_in_basic_yaml
        #add_a_station_code_list_in_basic_yaml(yaml_filename, key, a_new_station_code_list):
        add_a_station_code_list_in_basic_yaml(yaml_filename2, train_num, station_list)
        #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

#Code for option 2: searching by station code
else:
    for stn_code in station_list_for_searching:
        print(stn_code)
        #5.1 Departure
        get_departure_from_amtrak_to_dp_mysql_stncode(myvars["db_admin"], stn_code, start_date, end_date)
        #5.2 Arrival
        get_arrival_from_amtrak_to_dp_mysql_stncode(myvars["db_admin"], stn_code, start_date, end_date)
    
#==============================================================================
#------------------------------------------------------------------------------
#6) Retrieve data from WU to MySql
print("Step6:")
# Retrieve data from WU and store to dataframe from start_date to end_date
myvars2 = load_myvars_configurations(yaml_filename2)
station_list = myvars2["station_list"]
station_list_for_searching = myvars2["station_list_for_searching"]
train_number_list = myvars2["train_number_list"]

#Initial loop to collect data from WU during 
from datetime import date
start_date = date(int(myvars2["start_date"]["year"]), int(myvars2["start_date"]["month"]), int(myvars2["start_date"]["day"]))
end_date = date(int(myvars2["end_date"]["year"]), int(myvars2["end_date"]["month"]), int(myvars2["end_date"]["day"]))

# and then save to Mysql -> weather table after finished
#from retrieve_weather_data_from_WU import*
from retrieve_weather_data_from_WU_2020 import get_weather_data_by_astation_to_mysql

combined_stationlist = list()
#Function format: get_weather_data_by_astation_to_mysql(db_admin, a_station, in_start_date, in_end_date)
if (myvars2["searching_option"] == 1):
    combined_stationlist = list()
    for key, value in station_list.items():
        print("Retrieveing weather data of train number: "+ key)
        print(value)
        #Combined station list to avoid insert duplicate primary key for retrieveing weather data
        combined_stationlist = combined_stationlist + value
    
    #drop duplicate station code
    combined_stationlist = list(dict.fromkeys(combined_stationlist))
    
    for a_station in combined_stationlist:
        print(a_station)
        get_weather_data_by_astation_to_mysql(myvars["db_admin"], a_station ,start_date, end_date)

else:    
    for a_station in station_list_for_searching:
        for i_year in range (start_date.year, end_date.year+1):
            print(i_year)
            print(a_station)
            #V.2021 -> get and retrieve by 1 year
            #temp = 1 #round(((end_date.year - start_date.year)+1)/2)
            if i_year == start_date.year:
                start_date_loop = start_date
            else:
                start_date_loop = date(int(i_year), 1, 1)
                
            if i_year == end_date.year:
                end_date_loop = end_date
            else:
                end_date_loop  = date(int(i_year), 12, 31)
            
                
            #end_date_half = end_date.replace(year = (i_year+1))
            #start_date_half = date(int(end_date_half.year+1), 1, 1)

            #The first haft
            get_weather_data_by_astation_to_mysql(myvars["db_admin"], a_station ,start_date_loop, end_date_loop)
            
            import tqdm
            import time
            for i in tqdm.tqdm(range(int(2812*30/100))):
                time.sleep(0.01*3)

        '''
        #Delay
        print("")
        import tqdm
        import time
        #for i in tqdm.tqdm(range(2812*30)):
        for i in tqdm.tqdm(range(int(2812*30/100))):
            time.sleep(0.01*3)
        print("")    
        # The other half
        get_weather_data_by_astation_to_mysql(myvars["db_admin"], a_station ,start_date_half, end_date)
        
        print("")
        #for i in tqdm.tqdm(range(2812*30)):
        for i in tqdm.tqdm(range(int(2812*30/100))):
            time.sleep(0.01*2)
        # Full dowload within a time
        #get_weather_data_by_astation_to_mysql(myvars["db_admin"], a_station ,start_date, end_date)

        '''
#==============================================================================
# Test retrieveing missing data by date list
# import datetime
# date_plus_1 = start_date + datetime.timedelta(days=1)
start_date = datetime.date(2008, 1, 5)
#date_plus_1 = start_date + datetime.timedelta(days=1)
a_station = 'CHI'
get_weather_data_by_astation_to_mysql(myvars["db_admin"], a_station ,start_date, start_date)
            
#------------------------------------------------------------------------------
#==============================================================================
