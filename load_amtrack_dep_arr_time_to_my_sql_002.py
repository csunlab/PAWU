# -*- coding: utf-8 -*-
"""
@author: Pipatphon Lapamonpinyo
Web: https://csun.uic.edu/codes/PAWU.html
"""
#Retrieve data from https://juckins.net/amtrak_status/archive/html/history.php to CVS
import requests
import pandas as pd
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import datetime as dt
import numpy as np
import sqlalchemy as db

#==============================================================================
#------------------------------------------------------------------------------
def get_departure_from_amtrak_to_dp_mysql(db_admin, train_number, in_date_start, in_date_end ):
    #Inputs
    #db_admin = myvars["db_admin"]
    #train_number = "337"
    #date_start = "01%2F25%2F2008" #"01/25/2008" mm/dd/yyyy
    #date_end = "12%2F31%2F2019" #12/31/2019 -> 0-15955 results
    #temp = '01/25/2008'
    #in_date_start = '01/25/2008' #mm/dd/yyyy
    #in_date_end = '12/31/2009' #mm/dd/yyyy
    
    #in_date_start = '01/01/2017' #mm/dd/yyyy
    #in_date_end = '12/31/2019' #mm/dd/yyyy
    
    date_temp = pd.to_datetime(in_date_start)
    date_start = str(date_temp.month).zfill(2)+"%2F"+str(date_temp.day).zfill(2)+"%2F"+str(date_temp.year).zfill(4)
    
    date_temp = pd.to_datetime(in_date_end)
    date_end = str(date_temp.month).zfill(2)+"%2F"+str(date_temp.day).zfill(2)+"%2F"+str(date_temp.year).zfill(4)
    
    sort_dir = ["ASC", "DESC"]
    sort = ["schDp", "schAr"]
    
    #API ENDPOINT ()
    API_ENDPOINT = "https://juckins.net/amtrak_status/archive/html/history.php?train_num="+str(train_number) \
    +"&station=&date_start="+str(date_start)+"&date_end="+str(date_end)+\
    "&df1=1&df2=1&df3=1&df4=1&df5=1&df6=1&df7=1&sort="+sort[0]+\
    "&sort_dir="+ str(sort_dir[0]) +\
    "&co=gt&limit_mins=&dfon=1"
    
    url = API_ENDPOINT
    
    try:
        #html = requests.get(url).content
        html = requests.get(url).text
        df_list = pd.read_html(html)
    except requests.exceptions.RequestException as e:
        print(e)
        return e
    
    for i, df in enumerate(df_list):
        #print (df)
        df.to_csv('table {}.csv'.format(i))
    
    #Drop summary row
    #df.drop(df.index[0], inplace=True)
    
    #Drop last 2 average rows
    df = df[:-2]
    
    #Cleaning header of df to be the right format (without prefix)
    header_name = list(df) 
    header = [item[1] for item in header_name]
    df.columns = header
    
    #Retrieve weather data from WU by date and STNCODE and store results to df_main
    #M1) Create emyty DF to temporalily save data from WU (44 fields)
    #import sqlalchemy as db
    engine_text = 'mysql+pymysql://'+str(db_admin["user"])+':'+str(db_admin["passwd"])+'@'+str(db_admin["host"])+':3306/'+str(db_admin["db_name"])
    
    engine = db.create_engine(engine_text)
    connection = engine.connect()
   
    metadata = db.MetaData()
    #Load table "departure" by using db.Table()
    departure = db.Table('departure', metadata, autoload=True, autoload_with=engine)
    
    #Get column keys from weather table
    keys = departure.columns.keys()
    
    #Create "Empty df" so as to get train departure time (Sch_dp and Act_dp) from Amtack
    #import pandas as pd
    df_main = pd.DataFrame(columns=keys)
    
    #Add "Origon Date" from Amtrack to df_main, which in in the format for saving in MySql DB ("dp_date)
    #1)dp_date ----------------------------------------------------------------
    #df_main["dp_date"] = df["Origin Date"].str[:10]
    df_main["dp_date"] = pd.to_datetime(df["Origin Date"].str[:10]).dt.date
    
    #2)train_niumber ----------------------------------------------------------
    df_main["train_number"] = train_number
    
    #This function line requires "amtrack_stations.py" in the same folder
    from amtack_stations_02 import get_stnid_by_stncode_0
    #----------------------------------------------- (02/02/2020)
    
    #List unique station code 
    unique_stncode_keys = pd.Series(list(df.Station.unique()))
    #Use "get_stnid_by_stncode" function to inqure "STN_ID" from MySql DB
    #------------------------------------------------------------------------------(02/02/20)
    #unique_stncode_value = unique_stncode_keys.apply(get_stnid_by_stncode(db_admin))
    unique_stncode_value = unique_stncode_keys.apply(get_stnid_by_stncode_0)
    
    #Create dictionary for converting "STNCODE" to "STN_ID"
    dictionary = dict(zip(unique_stncode_keys, unique_stncode_value))
    
    #3)STN_ID -----------------------------------------------------------------
    #Save "STN_ID" after converted from "STNCODE" to af_main
    df_main["STN_ID"] = df["Station"].map(dictionary)
    
    #4)sch_dp -----------------------------------------------------------------
    #df["sch_dp"] = pd.to_datetime(df["Sch Dp"].str[:18])
    df_main["sch_dp"] = pd.to_datetime(df["Sch Dp"].str[:18]).dt.time
    
    #5)act_dp -----------------------------------------------------------------
    #df["act_dp"] = pd.to_datetime(df["Act Dp"])
    df_main["act_dp"] = pd.to_datetime(df["Act Dp"]).dt.time
    
    #6)comments
    df_main["comments"] = df["Comments"]
    
    #7)service_disruption
    df_main["service_disruption"] = df["Service Disruption"]
    
    #8)cancellations
    df_main["cancellations"] = df["Cancellations"]
    
    connection.close()
    engine.dispose()
    #'mysql+pymysql://root:1234@localhost:3306/amtack_stations'55
    engine_text = 'mysql+pymysql://'+str(db_admin["user"])+':'+str(db_admin["passwd"])+'@'+str(db_admin["host"])+':3306/'+str(db_admin["db_name"])
    
    try:
        #Apply sqlalchemy to store df_main to mySql
        #engine = db.create_engine('mysql+pymysql://root:1234@localhost:3306/amtack_stations')
        engine = db.create_engine(engine_text)
        connection = engine.connect()
        df_main.to_sql(name='departure', con=engine, if_exists = 'append', index=False)
        #Test one row - for the firs testing only - commented after tested 
        #df_main.loc[[0]].to_sql(name='departure', con=engine, if_exists = 'append', index=False)
        print("Successfully added departure data of train#"+str(train_number)+ \
              " from "+str(df_main["dp_date"][0])+" to "+str(df_main["dp_date"][len(df_main["dp_date"])-1])+\
              " to departure table in MySQL")
        connection.close()
        engine.dispose()
        return str("Successfully added departure data of train#"+str(train_number)+ \
              " from "+str(df_main["dp_date"][0])+" to "+str(df_main["dp_date"][len(df_main["dp_date"])-1])+\
              " to departure table in MySQL")
    except Exception as e:
        print(e)
        connection.close()
        engine.dispose()
        return str(e)

#==============================================================================
def get_departure_from_amtrak_to_dp_mysql_stncode(db_admin, stn_code, in_date_start, in_date_end ):
    #Inputs
    #train_number = "68"
    #date_start = "01%2F25%2F2008" #"01/25/2008" mm/dd/yyyy
    #date_end = "12%2F31%2F2018" #12/31/2018 -> 0-15955 results
    #temp = '01/25/2008'
    #in_date_start = '01/01/2008' #mm/dd/yyyy
    #in_date_end = '26/10/2009' #mm/dd/yyyy
    #in_date_start = '12/15/2018' #mm/dd/yyyy
    #in_date_end = '12/31/2018' #mm/dd/yyyy
    
    date_temp = pd.to_datetime(in_date_start)
    date_start = str(date_temp.month).zfill(2)+"%2F"+str(date_temp.day).zfill(2)+"%2F"+str(date_temp.year).zfill(4)
    
    date_temp = pd.to_datetime(in_date_end)
    date_end = str(date_temp.month).zfill(2)+"%2F"+str(date_temp.day).zfill(2)+"%2F"+str(date_temp.year).zfill(4)
    
    sort_dir = ["ASC", "DESC"]
    sort = ["schDp", "schAr"]
    
    #API ENDPOINT ()
    API_ENDPOINT = "https://juckins.net/amtrak_status/archive/html/history.php?train_num=&station=" \
    +str(stn_code)+"&date_start="+str(date_start)+"&date_end="+str(date_end)+\
    "&df1=1&df2=1&df3=1&df4=1&df5=1&df6=1&df7=1&sort="+sort[0]+\
    "&sort_dir="+ str(sort_dir[0]) +\
    "&co=gt&limit_mins=&dfon=1"
    
    #API_ENDPOINT2 = https://juckins.net/amtrak_status/archive/html/history.php?train_num=&station=CHI&date_start=04%2F03%2F2019&date_end=05%2F03%2F2019&df1=1&df2=1&df3=1&df4=1&df5=1&df6=1&df7=1&sort=schDp&sort_dir=DESC&co=gt&limit_mins=&dfon=1
    
    url = API_ENDPOINT
    
    try:
        html = requests.get(url).content
        df_list = pd.read_html(html)
    except requests.exceptions.RequestException as e:
        print(e)
        return e
    
    for i, df in enumerate(df_list):
        #print (df)
        df.to_csv('table {}.csv'.format(i))
    
    #Drop summary row
    #df.drop(df.index[0], inplace=True)
    
    #Drop last 2 average rows
    df = df[:-2]
    
    #Cleaning header of df to be the right format (without prefix)
    header_name = list(df) 
    header = [item[1] for item in header_name]
    df.columns = header
    
    #Retrieve weather data from WU by date and STNCODE and store results to df_main
    #M1) Create emyty DF to temporalily save data from WU (44 fields)
    #import sqlalchemy as db
    engine_text = 'mysql+pymysql://'+str(db_admin["user"])+':'+str(db_admin["passwd"])+'@'+str(db_admin["host"])+':3306/'+str(db_admin["db_name"])
    
    engine = db.create_engine(engine_text)
    connection = engine.connect()
    
    metadata = db.MetaData()
    #Load table "departure" by using db.Table()
    departure = db.Table('departure', metadata, autoload=True, autoload_with=engine)
    
    #Get column keys from weather table
    keys = departure.columns.keys()
    
    #Create "Empty df" so as to get train departure time (Sch_dp and Act_dp) from Amtack
    #import pandas as pd
    df_main = pd.DataFrame(columns=keys)
    
    #Add "Origon Date" from Amtrack to df_main, which in in the format for saving in MySql DB ("dp_date)
    #1)dp_date ----------------------------------------------------------------
    #df_main["dp_date"] = df["Origin Date"].str[:10]
    df_main["dp_date"] = pd.to_datetime(df["Origin Date"].str[:10]).dt.date
    
    #2)train_niumber ----------------------------------------------------------
    df_main["train_number"] = df["Train #"]
    
    '''
    #List unique station code 
    unique_stncode_keys = pd.Series(list(df.Station.unique()))
    #Use "get_stnid_by_stncode" function to inqure "STN_ID" from MySql DB
    unique_stncode_value = unique_stncode_keys.apply(get_stnid_by_stncode)
    #Create dictionary for converting "STNCODE" to "STN_ID"
    dictionary = dict(zip(unique_stncode_keys, unique_stncode_value))
    '''
    #3)STN_ID -----------------------------------------------------------------
    #This function line requires "amtrack_stations02.py" in the same folder
    from amtack_stations_02 import get_stnid_by_stncode
    stn_id = get_stnid_by_stncode(db_admin, stn_code)
    
    #Save "STN_ID" after converted from "STNCODE" to af_main
    df_main["STN_ID"] = stn_id
    
    #4)sch_dp -----------------------------------------------------------------
    #df["sch_dp"] = pd.to_datetime(df["Sch Dp"].str[:18])
    df_main["sch_dp"] = pd.to_datetime(df["Sch Dp"].str[:18]).dt.time
    
    #5)act_dp -----------------------------------------------------------------
    #df["act_dp"] = pd.to_datetime(df["Act Dp"])
    df_main["act_dp"] = pd.to_datetime(df["Act Dp"]).dt.time
    
    #6)comments
    df_main["comments"] = df["Comments"]
    
    #7)service_disruption
    df_main["service_disruption"] = df["Service Disruption"]
    
    #8)cancellations
    df_main["cancellations"] = df["Cancellations"]
    
    connection.close()
    engine.dispose()
    
    #'mysql+pymysql://root:1234@localhost:3306/amtack_stations'
    engine_text = 'mysql+pymysql://'+str(db_admin["user"])+':'+str(db_admin["passwd"])+'@'+str(db_admin["host"])+':3306/'+str(db_admin["db_name"])
    
    try:
        #Apply sqlalchemy to store df_main to mySql
        #engine = db.create_engine('mysql+pymysql://root:1234@localhost:3306/amtack_stations')
        engine = db.create_engine(engine_text)
        connection = engine.connect()
        df_main.to_sql(name='departure', con=engine, if_exists = 'append', index=False)
        #Test one row - for the firs testing only - commented after tested 
        #df_main.loc[[0]].to_sql(name='departure', con=engine, if_exists = 'append', index=False)
        print("Successfully added departure data of train station code "+str(stn_code)+ \
              " from "+str(df_main["dp_date"][0])+" to "+str(df_main["dp_date"][len(df_main["dp_date"])-1])+\
              " to departure table in MySQL")
        connection.close()
        engine.dispose()
        return str("Successfully added departure data of train station code "+str(stn_code)+ \
              " from "+str(df_main["dp_date"][0])+" to "+str(df_main["dp_date"][len(df_main["dp_date"])-1])+\
              " to departure table in MySQL")
    except Exception as e:
        print(e)
        connection.close()
        engine.dispose()
        return str(e)

#==============================================================================
#------------------------------------------------------------------------------
def get_arrival_from_amtrak_to_dp_mysql(db_admin, train_number, in_date_start, in_date_end ):
    #Inputs
    #train_number = 69
    #date_start = "01%2F25%2F2008" #"01/25/2008" mm/dd/yyyy
    #date_end = "12%2F31%2F2019" #12/31/2019 -> 0-15955 results
    #temp = '01/25/2008'
    #in_date_start = '01/25/2008' #mm/dd/yyyy
    #in_date_end = '12/31/2019' #mm/dd/yyyy

    date_temp = pd.to_datetime(in_date_start)
    date_start = str(date_temp.month).zfill(2)+"%2F"+str(date_temp.day).zfill(2)+"%2F"+str(date_temp.year).zfill(4)
    
    date_temp = pd.to_datetime(in_date_end)
    date_end = str(date_temp.month).zfill(2)+"%2F"+str(date_temp.day).zfill(2)+"%2F"+str(date_temp.year).zfill(4)
    
    sort_dir = ["ASC", "DESC"]
    sort = ["schDp", "schAr"]
    
    #API ENDPOINT ()
    API_ENDPOINT = "https://juckins.net/amtrak_status/archive/html/history.php?train_num="+str(train_number) \
    +"&station=&date_start="+str(date_start)+"&date_end="+str(date_end)+\
    "&df1=1&df2=1&df3=1&df4=1&df5=1&df6=1&df7=1&sort="+sort[1]+\
    "&sort_dir="+ str(sort_dir[0]) +\
    "&co=gt&limit_mins=&dfon=1"
    
    url = API_ENDPOINT
    
    try:
        #html = requests.get(url).content
        html = requests.get(url).text
        df_list = pd.read_html(html)
    except requests.exceptions.RequestException as e:
        print(e)
        return e
    
    for i, df in enumerate(df_list):
        #print (df)
        df.to_csv('table {}.csv'.format(i))
    
    #Drop summary row
    #df.drop(df.index[0], inplace=True)
    
    #Drop last 2 average rows
    df = df[:-2]
    
    #Cleaning header of df to be the right format (without prefix)
    header_name = list(df) 
    header = [item[1] for item in header_name]
    df.columns = header
    
    #Retrieve weather data from WU by date and STNCODE and store results to df_main
    #M1) Create emyty DF to temporalily save data from WU (44 fields)
    import sqlalchemy as db
    engine_text = 'mysql+pymysql://'+str(db_admin["user"])+':'+str(db_admin["passwd"])+'@'+str(db_admin["host"])+':3306/'+str(db_admin["db_name"])
    engine = db.create_engine(engine_text)
    connection = engine.connect()
    
    metadata = db.MetaData()
    #Load table "departure" by using db.Table()
    arrival = db.Table('arrival', metadata, autoload=True, autoload_with=engine)
    
    #Get column keys from weather table
    keys = arrival.columns.keys()
    
    #Create "Empty df" so as to get train departure time (Sch_dp and Act_dp) from Amtack
    #import pandas as pd
    df_main = pd.DataFrame(columns=keys)
    
    #Add "Origon Date" from Amtrack to df_main, which in in the format for saving in MySql DB ("dp_date)
    #1)dp_date ----------------------------------------------------------------
    #df_main["dp_date"] = df["Origin Date"].str[:10]
    df_main["ar_date"] = pd.to_datetime(df["Origin Date"].str[:10]).dt.date
    
    #2)train_niumber ----------------------------------------------------------
    df_main["train_number"] = train_number
    
    #Start: modified 02/08/20 -------------------------------------------------
    #This function line requires "amtrack_stations_02.py" in the same folder
    from amtack_stations_02 import get_stnid_by_stncode_0
    #----------------------------------------------- (02/02/2020)
    
    #List unique station code 
    unique_stncode_keys = pd.Series(list(df.Station.unique()))
    #Use "get_stnid_by_stncode" function to inqure "STN_ID" from MySql DB
    #------------------------------------------------------------------------------(02/02/20)
    #unique_stncode_value = unique_stncode_keys.apply(get_stnid_by_stncode(db_admin))
    unique_stncode_value = unique_stncode_keys.apply(get_stnid_by_stncode_0)
        
    #Create dictionary for converting "STNCODE" to "STN_ID"
    dictionary = dict(zip(unique_stncode_keys, unique_stncode_value))
    #----------------------------------------------------End: modified 02/08/20------
    
    '''
    #List unique station code 
    unique_stncode_keys = pd.Series(list(df.Station.unique()))
    #Use "get_stnid_by_stncode" function to inqure "STN_ID" from MySql DB
    unique_stncode_value = unique_stncode_keys.apply(get_stnid_by_stncode)
    #Create dictionary for converting "STNCODE" to "STN_ID"
    dictionary = dict(zip(unique_stncode_keys, unique_stncode_value))
    '''
    
    #3)STN_ID -----------------------------------------------------------------
    #Save "STN_ID" after converted from "STNCODE" to af_main
    df_main["STN_ID"] = df["Station"].map(dictionary)
    
    #4)sch_dp -----------------------------------------------------------------
    #df["sch_dp"] = pd.to_datetime(df["Sch Dp"].str[:18])
    df_main["sch_ar"] = pd.to_datetime(df["Sch Ar"].str[:18]).dt.time
    
    
    #5)act_dp -----------------------------------------------------------------
    #df["act_dp"] = pd.to_datetime(df["Act Dp"])
    df_main["act_ar"] = pd.to_datetime(df["Act Ar"]).dt.time
    
    #6)comments
    df_main["comments"] = df["Comments"]
    
    #7)service_disruption
    df_main["service_disruption"] = df["Service Disruption"]
    
    #8)cancellations
    df_main["cancellations"] = df["Cancellations"]
    
    connection.close()
    engine.dispose()
    
    try:
        #Apply sqlalchemy to store df_main to mySql
        engine = db.create_engine(engine_text)
        connection = engine.connect()
        df_main.to_sql(name='arrival', con=engine, if_exists = 'append', index=False)
        #Test one row - for the firs testing only - commented after tested 
        #df_main.loc[[0]].to_sql(name='departure', con=engine, if_exists = 'append', index=False)
        print("Successfully added departure data of train#"+str(train_number)+ \
              " from "+str(df_main["ar_date"][0])+" to "+str(df_main["ar_date"][len(df_main["ar_date"])-1])+\
              " to arrival table in MySQL")
        return str("Successfully added departure data of train#"+str(train_number)+ \
              " from "+str(df_main["ar_date"][0])+" to "+str(df_main["ar_date"][len(df_main["ar_date"])-1])+\
              " to arrival table in MySQL")
    except Exception as e:
        print(e)
        return str(e)
                
#==============================================================================
def get_arrival_from_amtrak_to_dp_mysql_stncode(db_admin, stn_code, in_date_start, in_date_end ):
    #Inputs
    #train_number = 68
    #date_start = "01%2F25%2F2008" #"01/25/2008" mm/dd/yyyy
    #date_end = "12%2F31%2F2018" #12/31/2018 -> 0-15955 results
    #temp = '01/25/2008'
    #in_date_start = '01/25/2008' #mm/dd/yyyy
    #in_date_end = '12/31/2019' #mm/dd/yyyy

    date_temp = pd.to_datetime(in_date_start)
    date_start = str(date_temp.month).zfill(2)+"%2F"+str(date_temp.day).zfill(2)+"%2F"+str(date_temp.year).zfill(4)
    
    date_temp = pd.to_datetime(in_date_end)
    date_end = str(date_temp.month).zfill(2)+"%2F"+str(date_temp.day).zfill(2)+"%2F"+str(date_temp.year).zfill(4)
    
    sort_dir = ["ASC", "DESC"]
    sort = ["schDp", "schAr"]
    
    #API ENDPOINT ()
    API_ENDPOINT = "https://juckins.net/amtrak_status/archive/html/history.php?train_num=&station="+str(stn_code) \
    +"&date_start="+str(date_start)+"&date_end="+str(date_end)+\
    "&df1=1&df2=1&df3=1&df4=1&df5=1&df6=1&df7=1&sort="+sort[1]+\
    "&sort_dir="+ str(sort_dir[0]) +\
    "&co=gt&limit_mins=&dfon=1"
    
    url = API_ENDPOINT
    
    try:
        html = requests.get(url).content
        df_list = pd.read_html(html)
    except requests.exceptions.RequestException as e:
        print(e)
        return e
    
    for i, df in enumerate(df_list):
        #print (df)
        df.to_csv('table {}.csv'.format(i))
    
    #Drop summary row
    #df.drop(df.index[0], inplace=True)
    
    #Drop last 2 average rows
    df = df[:-2]
    
    #Cleaning header of df to be the right format (without prefix)
    header_name = list(df) 
    header = [item[1] for item in header_name]
    df.columns = header
    
    #Retrieve weather data from WU by date and STNCODE and store results to df_main
    #M1) Create emyty DF to temporalily save data from WU (44 fields)
    import sqlalchemy as db  
    engine_text = 'mysql+pymysql://'+str(db_admin["user"])+':'+str(db_admin["passwd"])+'@'+str(db_admin["host"])+':3306/'+str(db_admin["db_name"])
    engine = db.create_engine(engine_text)
    connection = engine.connect()
    
    metadata = db.MetaData()
    #Load table "departure" by using db.Table()
    arrival = db.Table('arrival', metadata, autoload=True, autoload_with=engine)
    
    #Get column keys from weather table
    keys = arrival.columns.keys()
    
    #Create "Empty df" so as to get train departure time (Sch_dp and Act_dp) from Amtack
    #import pandas as pd
    df_main = pd.DataFrame(columns=keys)
    
    #Add "Origon Date" from Amtrack to df_main, which in in the format for saving in MySql DB ("dp_date)
    #1)dp_date ----------------------------------------------------------------
    #df_main["dp_date"] = df["Origin Date"].str[:10]
    df_main["ar_date"] = pd.to_datetime(df["Origin Date"].str[:10]).dt.date
    
    #2)train_niumber ----------------------------------------------------------
    df_main["train_number"] = df["Train #"]
    
    #3)STN_ID -----------------------------------------------------------------
    from amtack_stations_02 import get_stnid_by_stncode
    stn_id = get_stnid_by_stncode(db_admin, stn_code)
    
    #Save "STN_ID" after converted from "STNCODE" to af_main
    df_main["STN_ID"] = stn_id
    
    #4)sch_dp -----------------------------------------------------------------
    #df["sch_dp"] = pd.to_datetime(df["Sch Dp"].str[:18])
    df_main["sch_ar"] = pd.to_datetime(df["Sch Ar"].str[:18]).dt.time
    
    #5)act_dp -----------------------------------------------------------------
    #df["act_dp"] = pd.to_datetime(df["Act Dp"])
    df_main["act_ar"] = pd.to_datetime(df["Act Ar"]).dt.time
    
    #6)comments
    df_main["comments"] = df["Comments"]
    
    #7)service_disruption
    df_main["service_disruption"] = df["Service Disruption"]
    
    #8)cancellations
    df_main["cancellations"] = df["Cancellations"]
    
    connection.close()
    engine.dispose()
    
    try:
        #Apply sqlalchemy to store df_main to mySql
        engine = db.create_engine(engine_text)
        connection = engine.connect()
        df_main.to_sql(name='arrival', con=engine, if_exists = 'append', index=False)
        #Test one row - for the firs testing only - commented after tested 
        #df_main.loc[[0]].to_sql(name='departure', con=engine, if_exists = 'append', index=False)
        print("Successfully added departure data of station code: "+str(stn_code)+ \
              " from "+str(df_main["ar_date"][0])+" to "+str(df_main["ar_date"][len(df_main["ar_date"])-1])+\
              " to arrival table in MySQL")
        connection.close()
        engine.dispose()
        return str("Successfully added departure data of station code: "+str(stn_code)+ \
              " from "+str(df_main["ar_date"][0])+" to "+str(df_main["ar_date"][len(df_main["ar_date"])-1])+\
              " to arrival table in MySQL")
    except Exception as e:
        print(e)
        connection.close()
        engine.dispose()
        return str(e)

#==============================================================================
#--------------------------------- Main ---------------------------------------
#==============================================================================
#To get departure dati of train#337 to "departure" table in MySQL
'''
d_start = '01/25/2008'
d_end = '12/31/2018'

d_start = '1/6/2014'
d_end = '1/8/2014'

train_num = 290

get_departure_from_amtrak_to_dp_mysql(train_num, d_start, d_end)
#get_arrival_from_amtrak_to_dp_mysql(train_num, d_start, d_end)
'''
