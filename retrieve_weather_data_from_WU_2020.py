# -*- coding: utf-8 -*-
"""
@author: Pipatphon Lapamonpinyo
Web: https://csun.uic.edu/codes/PAWU.html
"""
#==============================================================================
def load_myvars_configurations(yaml_filename):
    import yaml
    with open(yaml_filename, 'r', encoding='utf8') as stream:
        try:
            myvars = yaml.safe_load(stream)
            #print("Successfully loaded vars from \""+ yaml_filename+"\" to \"myvars\"!\n")
            return myvars
        except yaml.YAMLError as exc:
            print(exc)
#==============================================================================
def load_myvars_configurations(yaml_filename):
    import yaml
    with open(yaml_filename, 'r', encoding='utf8') as stream:
        try:
            myvars = yaml.safe_load(stream)
            #print("Successfully loaded vars from \""+ yaml_filename+"\" to \"myvars\"!\n")
            return myvars
        except yaml.YAMLError as exc:
            print(exc)
#------------------------------------------------------------------------------
#Define function  "get_stnid_stncode(stncode)" require stncode // station_code
def get_stnid_by_stncode(db_admin, stncode):
    import sqlalchemy as db
    engine_text = 'mysql+pymysql://'+str(db_admin["user"])+':'+str(db_admin["passwd"])+'@'+str(db_admin["host"])+':3306/'+str(db_admin["db_name"])
    engine = db.create_engine(engine_text)
    #engine = db.create_engine('mysql+pymysql://root:1234@localhost:3306/amtack_stations')
    connection = engine.connect()
    metadata = db.MetaData()
    amtack_stations = db.Table(db_admin["stations_table_name"], metadata, autoload=True, autoload_with=engine)
    
    #print(amtack_stations.columns.keys())
    #['OBJECTID', 'STNCODE', 'STNNAME', 'CITY2', 'STATE', 'STFIPS', 'Latitude', 'Longitude']

    query = db.select([amtack_stations.columns.STN_ID]).where(amtack_stations.columns.STNCODE == stncode)
    
    ResultProxy = connection.execute(query)
    ResultSet = ResultProxy.fetchall()
    import pandas as pd
    df = pd.DataFrame(ResultSet)
    
    connection.close()
    engine.dispose()
    
    if df.empty:
        return -9999    #no station found
    else:
        df.columns = ResultSet[0].keys()
        return df.STN_ID[0]

#Test function get_stnid_stncode(stncode) #Test function-----------------------Test
'''
#Require stn_code for searching STN_ID by calling get_stnid_stncode(stncode)
stn_code = 'CHI' 
stn_id = get_stnid_by_stncode(db_admin, stn_code)
print("STN_ID of", stn_code, "is", stn_id)
'''

#==============================================================================
#------------------------------------------------------------------------------
#define function // long, lat = get_longlat_by_stncode(stn_code) //
def get_longlat_by_stncode(db_admin, stncode):
    #Create engine to connect python to mysql by using sqlalchemy as db package
    import sqlalchemy as db
    engine_text = 'mysql+pymysql://'+str(db_admin["user"])+':'+str(db_admin["passwd"])+'@'+str(db_admin["host"])+':3306/'+str(db_admin["db_name"])
    engine = db.create_engine(engine_text)
    #engine = db.create_engine('mysql+pymysql://root:1234@localhost:3306/amtack_stations')
    connection = engine.connect()
    metadata = db.MetaData()
    #Load table "amtack_stations" by using db.Table()
    amtack_stations = db.Table(db_admin["stations_table_name"], metadata, autoload=True, autoload_with=engine)
    
    #print(amtack_stations.columns.keys())
    #['OBJECTID', 'STNCODE', 'STNNAME', 'CITY2', 'STATE', 'STFIPS', 'Latitude', 'Longitude']
    
    #Variables for in-function testing only (comment after tested) ***
    #stncode = "NYP"
    
    #Query long and Lat from 'amtack_stations' db in MySql
    query = db.select([amtack_stations.columns.Longitude, amtack_stations.columns.Latitude]).where(amtack_stations.columns.STNCODE == stncode)
    
    ResultProxy = connection.execute(query)
    ResultSet = ResultProxy.fetchall()
    import pandas as pd
    df = pd.DataFrame(ResultSet)
    
    connection.close()
    engine.dispose()
    
    if df.empty:
        return -9999, -9999    #no station found
    else:
        df.columns = ResultSet[0].keys()
        return df.Longitude[0], df.Latitude[0]  
    
#Test function get_longlat_by_stncode(stncode) #Test function------------------Test
'''
#Require stn_code for searching Long & Lat by calling get_longlat_by_stncode(stncode)
stn_code = 'CHI' 
stn_long, stn_lat = get_longlat_by_stncode(stn_code)
print("Long and Lat of ",stn_code," is",stn_long,",",stn_lat)
'''
#==============================================================================
'''
def check_url_request_200_from_lat_long(lat_list, long_list):
    db_admin = myvars["db_admin"]
    import requests
    #url_almaac = "https://api.weather.com/v1/geocode/41.8787/-87.6394/almanac/daily.json?apiKey=6532d6454b8aa370768e63d6ba5a832e&units=m&start=0603"
    #url = "https://api.weather.com/v1/geocode/41.8787/-87.6394/observations/historical.json?apiKey=6532d6454b8aa370768e63d6ba5a832e&units=e&startDate=20130603"
    url = "https://api.weather.com/v1/geocode/"+str(stn_long)+"/"+str(stn_lat)+"/observations/historical.json?apiKey="+str(wu_api_key)+"&units=e&startDate="+str(date)
    
    from fake_useragent import UserAgent
    ua = UserAgent()
    headers = {'User-Agent': ua.chrome}
    url_request = requests.get(url, headers=headers).json()
'''    
#==============================================================================    
#------------------------------------------------------------------------------
# Define function to retrieve historical data from WU
# Retrieve historical weather information from WU by location(c_long, c_lat)
def get_weather_by_date_stncode_to_mysql(db_admin, date, stncode):
    # stn_code for internal testing only -> commented after testing ***
    #stncode = 'DOA'
    #Step 1) get STN_ID from weather DB by stncode
    #Call function*************************************************************
    #db_admin = myvars["db_admin"]
    stn_id = get_stnid_by_stncode(db_admin, stncode)
    
    #Step 2) get location (long and lat) of the interesting station
    #Call function*************************************************************
    stn_long, stn_lat = get_longlat_by_stncode(db_admin, stncode)
    
    #Step 3) retrieve historical weather information from WU by calling API V2
    ###########################################################################
    #Variables for in-function testing only (comment after tested) ***
    #3.1 date must be defined ***
    
    # date for internal testing only -> commented after testing ***
    #date = '20100903'
    #date = '20081231'
    
    #3.2 Location (lat and long) of a specific station must be defined ***
    #stn_long = '41.87868'
    #stn_lat = '-87.63944'
    #Form the right format of lacation (coordinate by long and lat)
    #location = str(stn_long)+','+str(stn_lat)     # format -> '41.87868,-87.63944'

    #Add code for retrieving user's api_key
    #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    #Loading pre-definded train_no, start_date, end_date
    yaml_filename0 = "amtrak_vars2.yaml"    
    print("Loading pre-defined WU-api_key from \""+ yaml_filename0+ "\"...")
    #function -> load_myvars_configurations(yaml_filename)
    myvars0 = load_myvars_configurations(yaml_filename0)
    #6532d6454b8aa370768e63d6ba5a832e
    wu_api_key = str(myvars0["wu_api_key"])
    #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    #import urllib
    #from urllib.request import urlopen
    #import json
    
    #New API Service updated 03/10/2020
    import requests
    #url_almaac = "https://api.weather.com/v1/geocode/41.8787/-87.6394/almanac/daily.json?apiKey=6532d6454b8aa370768e63d6ba5a832e&units=m&start=0603"
    #url = "https://api.weather.com/v1/geocode/41.8787/-87.6394/observations/historical.json?apiKey=6532d6454b8aa370768e63d6ba5a832e&units=e&startDate=20130603"
    url = "https://api.weather.com/v1/geocode/"+str(stn_long)+"/"+str(stn_lat)+"/observations/historical.json?apiKey="+str(wu_api_key)+"&units=e&startDate="+str(date)
    
    from fake_useragent import UserAgent
    ua = UserAgent()
    headers = {'User-Agent': ua.chrome}
    url_request = requests.get(url, headers=headers).json()
    
    #add alternative lat and long to find the closest weather station avaiable
    if (url_request['metadata']['status_code'] != 200):
        #get alternative_lat_long
        url_choice = "https://api.weather.com/v3/location/near?geocode="+str(stn_long)+","+str(stn_lat)+"&product=observation&format=json&apiKey="+str(wu_api_key)
        url_request_choice = requests.get(url_choice, headers=headers).json()
        alt_lat_list = url_request_choice['location']['latitude']
        alt_long_list = url_request_choice['location']['longitude']
        alt_w_stnid_list = url_request_choice['location']['stationId']
        alt_w_stn_distanceKm_list = url_request_choice['location']['distanceKm']
        for i in range(0, len(alt_lat_list)):
            url_alt = "https://api.weather.com/v1/geocode/"+str(alt_lat_list[i])+"/"+str(alt_long_list[i])+"/observations/historical.json?apiKey="+str(wu_api_key)+"&units=e&startDate="+str(date)
            #from fake_useragent import UserAgent
            ua = UserAgent()
            headers = {'User-Agent': ua.chrome}
            url_request_alt = requests.get(url_alt, headers=headers).json()
            
            #Check if alternative weather station query is avalable an successfully retrieved
            if (url_request_alt['metadata']['status_code'] == 200):
                #check if alt distanceKm <= 50 km
                if (alt_w_stn_distanceKm_list[i] <= 50):
                    url_request = url_request_alt
                    print("Successfully changed weather station from "+alt_w_stnid_list[0]+" to be "+ alt_w_stnid_list[i])
                    break
                else:
                    print("Alt weather station is out of range!")
            else:
                print("Weather data from "+ alt_w_stnid_list[i]+" station is not found")

        
    if (url_request['metadata']['status_code'] == 200):
        #OK,The request has succeded:
        all_observations = url_request["observations"] # The whole data in the table
   
        #Extract all variables from daily summary 
        #dict1 = {"number of storage arrays": 45, "number of ports":2390}
        import pandas as pd
        df_all_observations = pd.DataFrame(all_observations)
        df_all_observations = df_all_observations.drop(['key', 'class', 'blunt_phrase', 'terse_phrase'], 1)
        
        #insert column w_date and STN_ID
        df_all_observations['w_date'] = date
        df_all_observations['STN_ID'] = stn_id
        
        #convert str to TIMESTAMP
        #df_all_observations['expire_time_gmt'] = pd.to_datetime(df_all_observations['expire_time_gmt'], unit='s', utc=True)
        #df_all_observations['valid_time_gmt'] = pd.to_datetime(df_all_observations['valid_time_gmt'], unit='s', utc=True)
    
        #import datetime
        #datetime.datetime.fromtimestamp(1203925860)
        #from pytz import UTC
        #from pytz import timezone
        #import datetime as dt
        #df['ts'] = df.datetime.values.astype(np.int64)
    ###########################################################################
    #Return 
    #insert weather data (a new record) to the weather table in MySql
    
    #Create engine to connect python to mysql by using sqlalchemy as db package
    import sqlalchemy as db
    engine_text = 'mysql+pymysql://'+str(db_admin["user"])+':'+str(db_admin["passwd"])+'@'+str(db_admin["host"])+':3306/'+str(db_admin["db_name"])
    engine = db.create_engine(engine_text)
    
    #engine = db.create_engine('mysql+pymysql://root:1234@localhost:3306/amtack_stations')
    connection = engine.connect()
    metadata = db.MetaData()
    #Load table "amtack_stations" by using db.Table()
    weather = db.Table(db_admin["weather_table_name"], metadata, autoload=True, autoload_with=engine)
    #db_admin = myvars["db_admin"]
    
    #Get column keys from weather table
    keys = weather.columns.keys()
    
    #Create "Empty df" so as to get weather information (44 fileds) in the sext step
    import pandas as pd
    return_df = pd.DataFrame(columns=keys, index = [0])
    
    if (url_request['metadata']['status_code'] == 200):
        #OK,The request has succeded:
        #Rearranging return_df
        return_df = df_all_observations[list(keys)]
    else:
        return_df = pd.DataFrame()
    
    connection.close()
    engine.dispose()
    
    #return return_df to the outer
    return return_df

#---------------------------- End of Function --------------------------------#
def add_w_by_stncode_to_df (db_admin, df_main, in_year, in_month, in_day, stn_code):
    #stn_code = 'LDB'
    #in_year = 2008
    #in_month = 1
    #in_day = 25
    #check_empty
    #stn_code = 'GLN'
    #in_year = 2008
    #in_month = 9
    #in_day = 12
    year = in_year
    month = in_month
    day = in_day
    date = str(year)+str(month).zfill(2)+str(day).zfill(2)   #'20090826'
    #Call FUnction*************************************************************
    df_temp = get_weather_by_date_stncode_to_mysql(db_admin, date, stn_code)

    #Merge a new data row (df_temp) into the existing df_main by unising "pd.concat"
    #@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    '''
    #Check if the data does not exist
    form_df_date = (str(year)+'-'+str(month).zfill(2)+'-'+str(day).zfill(2))
    #Call function*************************************************************
    stn_id = get_stnid_by_stncode(db_admin, stn_code)
    #find_exist_index = df_main.index[df_main['w_date'] == form_df_date].tolist()
    # Must excute thw two line below for checking if the row does exist
    
    df_check_existing = df_main[(df_main['w_date'] == form_df_date) & (df_main['STN_ID'] == stn_id)]
    
    import pandas as pd
    if (df_check_existing.empty):
        df_main = pd.concat([df_main, df_temp], axis = 0)
    '''
    #@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    
    import pandas as pd
    df_main = pd.concat([df_main, df_temp], axis = 0)
    # Double check drop if duplicated
    df_main = df_main.drop_duplicates(keep='first', inplace=False)
    
    return df_main

#------------------------------------------------------------------------------
#User-defined function for getting weather informationm of each station from "weather" table in mySql
def get_weather_by_stationcode_from_mysql(db_admin, stn_code):
    #station_id = 4
    import sqlalchemy as db
    engine_text = 'mysql+pymysql://'+str(db_admin["user"])+':'+str(db_admin["passwd"])+'@'+str(db_admin["host"])+':3306/'+str(db_admin["db_name"])
    engine = db.create_engine(engine_text)
    connection = engine.connect()
    
    metadata = db.MetaData()
    #Load table "departure" by using db.Table()
    weather = db.Table(db_admin["weather_table_name"], metadata, autoload=True, autoload_with=engine)
    
    stn_id = get_stnid_by_stncode(db_admin, stn_code)
    query = db.select([weather.columns.w_date, weather.columns.STN_ID]).where(db.and_(weather.columns.STN_ID == int(stn_id))).order_by(db.asc(weather.columns.w_date))
    
    ResultProxy = connection.execute(query)
    ResultSet = ResultProxy.fetchall()
    
    #COnvert raw data from MySql to DF
    import pandas as pd
    df = pd.DataFrame(ResultSet)
    df.columns = ResultSet[0].keys()
    
    connection.close()
    engine.dispose()
    return df

###############################################################################
#                             Main Program   
###############################################################################
#Modified code to be used with GUI concept, a_station = STN_CODE
#------------------------------------------------------------------------------
def get_weather_data_by_astation_to_mysql(db_admin, a_station, in_start_date, in_end_date ):
    #in_start_date = '01/01/2008' #mm/dd/yyyy
    #in_end_date = '12/31/2008' #mm/dd/yyyy
    
    #Check Empty
    #in_start_date = '09/13/2008' #mm/dd/yyyy
    #in_end_date = '09/14/2008' #mm/dd/yyyy
    
    #Check insert to mySQL error
    #in_start_date = '02/25/2008' #mm/dd/yyyy
    #in_end_date = '03/10/2008' #mm/dd/yyyy
    
    #2011-01-23
    #in_start_date = '01/22/2011' #mm/dd/yyyy
    #in_end_date = '01/25/2011' #mm/dd/yyyy
    
    # and then save to Mysql -> weather table after finished
    import pandas as pd
    daterange = pd.date_range(in_start_date, in_end_date)
    
    #Retrieve weather data from WU by date and STNCODE and store results to df_main
    #M1) Create emyty DF to temporalily save data from WU (44 fields)
    import sqlalchemy as db
    engine_text = 'mysql+pymysql://'+str(db_admin["user"])+':'+str(db_admin["passwd"])+'@'+str(db_admin["host"])+':3306/'+str(db_admin["db_name"])
    engine = db.create_engine(engine_text)
    
    #engine = db.create_engine('mysql+pymysql://root:1234@localhost:3306/amtack_stations')
    connection = engine.connect()
    metadata = db.MetaData()
    #Load table "weather" by using db.Table()
    weather = db.Table(db_admin["weather_table_name"], metadata, autoload=True, autoload_with=engine)
    
    #Get column keys from weather table
    keys = weather.columns.keys()
    
    connection.close()
    engine.dispose()
    
    #Create "Empty df" so as to get weather information (44 fileds) in the sext step
    import pandas as pd
    df_main = pd.DataFrame(columns=keys)  

    for single_date in daterange:
        year = single_date.strftime("%Y")
        month = single_date.strftime("%m")
        day = single_date.strftime("%d")
        print (single_date.strftime("%Y-%m-%d"))
        print (a_station)
        #Call function*********************************************************
        if (month == 1) and (day == 1):
            import time
            time.sleep(60)
        #Have to fix the api_key--------------------------------------------------------
        df_main = add_w_by_stncode_to_df(db_admin, df_main, year, month, day, a_station)
    
    #Reset dataframe index 
    df_main = df_main.reset_index(drop=True)
    #M2) Apply sqlalchemy to store df_main to mySql
    #engine = db.create_engine('mysql+pymysql://root:1234@localhost:3306/amtack_stations')
    #engine = db.create_engine(engine_text)
    #df_main.to_sql(name='weather', con=engine, if_exists = 'append', index=False)
    
    try:
        #Apply sqlalchemy to store df_main to mySql
        #engine = db.create_engine('mysql+pymysql://root:1234@localhost:3306/amtack_stations')
        engine_text = 'mysql+pymysql://'+str(db_admin["user"])+':'+str(db_admin["passwd"])+'@'+str(db_admin["host"])+':3306/'+str(db_admin["db_name"])
        engine = db.create_engine(engine_text)
        #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        #Add code to check if data has already existed
        '''
        df_check = get_weather_by_stationcode_from_mysql(db_admin, a_station)
        if not (df_check.empty):
            from datetime import datetime
            df_main2 = df_main[df_main["w_date"].isin(df_check["w_date"])].dropna()
            #pd.to_datetime(z).date()
            x = pd.to_datetime(df_check["w_date"])
            #df_stn_1 = df_stn_1.drop(df_stn_1.index[df_stn_1['dp_date'].isin(missing_date[0])].tolist())
            df_main2 = df_main.drop(df_main.index[df_main['w_date'].isin(pd.to_datetime(df_check["w_date"]).date())].tolist())
        '''
        #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        if not (df_main.empty):
            df_main.to_sql(name='weather', con=engine, if_exists = 'append', index=False)
            #Test one row - for the firs testing only - commented after tested 
            #df_main.loc[[0]].to_sql(name='departure', con=engine, if_exists = 'append', index=False)
            print("Successfully added weather data of "+str(a_station)+ \
                  "\" station from "+str(df_main["w_date"][0])+" to "+str(df_main["w_date"][len(df_main["w_date"])-1])+\
                  " to weather table in MySQL")
            
            connection.close()
            engine.dispose()
            return str("Successfully added weather data of "+str(a_station)+ \
                  "\" station from "+str(df_main["w_date"][0])+" to "+str(df_main["w_date"][len(df_main["w_date"])-1])+\
                  " to weather table in MySQL")
        else:
            print("All weather data from "+str(in_start_date)+" to "+str(in_end_date)+" of "+str(a_station)+" station have already existed!\n")
    except Exception as e:
        print(e)
        connection.close()
        engine.dispose()
        return str(e)
