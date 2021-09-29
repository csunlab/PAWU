# -*- coding: utf-8 -*-
"""
@author: Pipatphon Lapamonpinyo
Web: https://csun.uic.edu/codes/PAWU.html
"""
#-----------------------------------------------------------------------------------------
def load_myvars_configurations(yaml_filename):
    import yaml
    with open(yaml_filename, 'r', encoding='utf8') as stream:
        try:
            myvars = yaml.safe_load(stream)
            print("Successfully loaded vars from \""+ yaml_filename+"\" to \"myvars\"!\n")
            return myvars
        except yaml.YAMLError as exc:
            print(exc)

#-----------------------------------------------------------------------------------------
def create_mydb(db_admin):    
    import mysql.connector
    #create mySql conector
    print("Creating "+db_admin["db_name"]+"...")
    mydb = mysql.connector.connect(
      host= db_admin["host"],
      user = db_admin["user"],
      passwd= db_admin["passwd"]
    )
    print("Mysql connector is connected!")

    mycursor = mydb.cursor()
    try:
        mycursor.execute("CREATE DATABASE "+db_admin["db_name"])
        print("Your database \""+db_admin["db_name"]+"\" is successfully created!\n")
        return "Your database \""+db_admin["db_name"]+"\" is successfully created!\n"
    except mysql.connector.Error as err:
        print("Something went wrong: {}\n".format(err))
        return err

#-----------------------------------------------------------------------------------------
#Create table with single pk and without foreign key constrains
def create_table(db_admin, my_table_name, field_list, pk, fk):
    print("Creating "+my_table_name+" table ...")
    
    #create mySql conector
    import mysql.connector
    mydb = mysql.connector.connect(
      host= db_admin["host"],
      user = db_admin["user"],
      passwd= db_admin["passwd"],
      database= db_admin["db_name"]
    )
    mycursor = mydb.cursor()
    
    #Prepare text to create a table in Mysql
    if isinstance(next(iter(fk or []), None), type(None)):
        create_table_text = 'CREATE TABLE '+str(my_table_name)+ \
                        '(' + ','.join(field_list) + \
                        ',PRIMARY KEY ('+ ','.join(pk) +')' +\
                        ') ENGINE = INNODB'
    else:
        create_table_text = 'CREATE TABLE '+str(my_table_name)+ \
                        '(' + ','.join(field_list) + \
                        ',PRIMARY KEY ('+ ','.join(pk) +')' +\
                        ',FOREIGN KEY ('+ fk['col'] +') REFERENCES '+ fk['ref_table'] +\
                        ' (' + fk['col'] +') ON DELETE CASCADE ON UPDATE NO ACTION' +\
                        ') ENGINE = INNODB'
    
    try:
        mycursor.execute(create_table_text)
        print("Your table \"" +my_table_name+ "\" is successfully created!\n")
        return "Your table \"" +my_table_name+ "\" is successfully created!\n"
    except mysql.connector.Error as err:
        print("Something went wrong: {}\n".format(err))
        return err

#-----------------------------------------------------------------------------------------
def drop_table(db_admin, table_name):
    import mysql.connector
    mydb = mysql.connector.connect(
            host= db_admin["host"],
            user = db_admin["user"],
            passwd= db_admin["passwd"],
            database= db_admin["db_name"]
    )
    
    mycursor = mydb.cursor()
    sql = "DROP TABLE IF EXISTS "+str(table_name)
    mycursor.execute(sql)

#=========================================================================================
#                                    End of Modules
#=========================================================================================
