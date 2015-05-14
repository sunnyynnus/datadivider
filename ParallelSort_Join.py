#!/usr/bin/python2.7
#
# Implemented two methods
# Parallel Sort
# Parallel Join
#
import os, sys
import psycopg2
import threading

DATABASE_NAME = 'dds_assgn'


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    #pass
    cur = openconnection.cursor()
    try:
        #drop table if exists
        cur.execute("DROP TABLE IF EXISTS "+ ratingstablename)

        #creating table
        cur.execute("CREATE TABLE "+ ratingstablename +" (p_id serial primary key, user_id integer, delim_temp_1 char(2), movie_id integer, delim_temp_2 char(2), rating real, delim_temp_3 char(2), timestamp varchar)")
        #ratingsfilepath="ratings.dat"
        file = open(ratingsfilepath, 'rb')
        ratingsfilepath= os.path.abspath(file.name)
        prepareQuery = "COPY "+ ratingstablename +" (user_id, delim_temp_1, movie_id, delim_temp_2, rating, delim_temp_3, timestamp) FROM '"+ ratingsfilepath +"' (FORMAT text, DELIMITER (':'))"

        cur.execute(prepareQuery)
        cur.execute("ALTER TABLE "+ ratingstablename +" DROP column delim_temp_1,DROP column delim_temp_2, DROP column delim_temp_3")

        openconnection.commit()
        cur.close()

        print("*********Loading Completed *********")
    except Exception as detail:
        print("problem in load is --", detail)
        #sys.exit()


############################ Parallel Sort ###############################

def ParallelSort(Table, SortingColumnName, OutputTable, openconnection):
    try:
        ratingstablename= Table
        columnname = SortingColumnName
        outputtablename = OutputTable
        cursor=openconnection.cursor()
        cursor.execute("SELECT MIN("+columnname+") FROM "+ratingstablename+"")
        minval=cursor.fetchone()
        range_s=(float)(minval[0])
        
        cursor.execute("SELECT MAX("+columnname+") FROM "+ratingstablename+"")
        maxval=cursor.fetchone()
        tempmax= (float) (maxval[0])
        interval=(tempmax-range_s)/5
        range_e=range_s+interval
        
        cursor.execute("DROP TABLE IF EXISTS "+ outputtablename)
        cursor.execute("CREATE TABLE "+outputtablename+" (like "+ratingstablename+")")
        cursor.execute("ALTER TABLE "+ outputtablename+" ADD TupleOrder integer")

        t=[0 for i in range(5)]
        for i in range(0, 5):
            cursor.execute("SELECT COUNT(*) FROM "+ratingstablename+" WHERE "+columnname+">="+str(range_s)+" and "+columnname+"<"+str(range_e)+"")
            partitionsize=cursor.fetchone()
            count=int(partitionsize[0])
            if(i==0):
                mincount= 1
                maxcount = count
            else:
                mincount = maxcount+1
                maxcount = mincount+count-1
            
            ## insert using thread
            #start_new_thread for each virtual partition
            t[i]= threading.Thread(target=range_part_insert, args=(ratingstablename, outputtablename, columnname, i, mincount-1, range_s, range_e, openconnection))
            t[i].start()

            range_s= range_e
            range_e= range_s+interval
            if(i==3):
                range_e+=1 # don't want to loose max value

        for i in range(0, 5):
            t[i].join()
        openconnection.commit()
        cursor.close()
        print("******* Parallel Sort Completed**********")
    except Exception as detail:
        print("problem in ParallelSort is --", detail)


def range_part_insert(ratingstablename, outputtablename, columnname, i, mincount, minval, maxval, openconnection):
    try:
        cursor=openconnection.cursor()
        
        insertquery= "INSERT INTO "+outputtablename+ " select *,row_number() over(order by "+columnname+")+"+str(mincount)+" r  from "+ratingstablename+"  WHERE "+columnname+">="+str(minval)+" and "+columnname+"<"+str(maxval)+" ORDER BY "+columnname+""
        
        cursor.execute(insertquery)
        openconnection.commit()
        cursor.close()
    except Exception as detail:
        print("problem in range partition insertion is --", detail)


######################## ParallelJoin  ##################

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    try:
        cursor=openconnection.cursor()
        
        # Find range
        cursor.execute("SELECT MIN("+Table1JoinColumn+") FROM "+InputTable1+"")
        minval=cursor.fetchone()
        range_s1=(float)(minval[0])
        cursor.execute("SELECT MIN("+Table2JoinColumn+") FROM "+InputTable2+"")
        minval=cursor.fetchone()
        range_s2=(float)(minval[0])
        min_range=0.0
        if(range_s1 > range_s2):
            min_range = range_s1
        else:
            min_range= range_s2
        cursor.execute("SELECT MAX("+Table1JoinColumn+") FROM "+InputTable1+"")
        maxval=cursor.fetchone()
        max_range_1=(float)(maxval[0])
        cursor.execute("SELECT MAX("+Table2JoinColumn+") FROM "+InputTable2+"")
        maxval=cursor.fetchone()
        max_range_2=(float)(maxval[0])
        max_range= 0.0
        if(max_range_1 < max_range_2):
            max_range = max_range_1
        else:
            max_range = max_range_2
        interval= (max_range - min_range)/5
        
        range_s = min_range
        range_e= range_s+interval

        t=[0 for i in range(5)]
        for i in range(0, 5):
            
            ## insert using thread
            #start_new_thread for each virtual partition
            t[i]= threading.Thread(target=range_part_join_insert, args=(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, i, range_s, range_e, openconnection))
            t[i].start()
            
            range_s= range_e
            range_e= range_s+interval
            if(i==3):
                range_e+=1 # don't want to loose max value
            
        # wait for threads to finish
        for i in range(0, 5):
            t[i].join()

        ## partition tables created
        ## create output table schema
        cursor.execute("DROP TABLE IF EXISTS "+OutputTable)
        cursor.execute("select column_name, data_type from information_schema.columns where table_name='"+InputTable1+"'")
        columns_table1 = cursor.fetchall()
        output_schema = "("
        for i in range(len(columns_table1)):
            output_schema += columns_table1[i][0]+ " " + columns_table1[i][1]+","
            
        cursor.execute("select column_name, data_type from information_schema.columns where table_name='"+InputTable2+"'")
        columns_table2 = cursor.fetchall()
        for i in range(len(columns_table2)):
            if(i == len(columns_table2)-1):
                output_schema += columns_table1[i][0] +"1"+" " + columns_table1[i][1]
            else:
                output_schema += columns_table1[i][0]+ "1"+ " " + columns_table1[i][1]+","
        output_schema += ")"

        output_table_query= "CREATE TABLE "+OutputTable+" "+output_schema
        #print(output_table_query)
        cursor.execute(output_table_query)

        openconnection.commit()

        for i in range(0, 5):
            t[i] = threading.Thread(target=parallel_join, args=(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, i,openconnection))
            t[i].start();

        # wait for threads to finish
        for i in range(0, 5):
            t[i].join()

        openconnection.commit()
        cursor.close()
        print("*********** Parallel Join completed ***********")
    except Exception as detail:
        print("problem in Parallel Join is --", detail)


def range_part_join_insert(inputtable1, inputtable2, table1joincolumn, table2joincolumn, i, min_value, max_value, openconnection):
    try:
        cursor=openconnection.cursor()
        
        cursor.execute("DROP TABLE IF EXISTS inputtable1_partition"+str(i))
        cursor.execute("DROP TABLE IF EXISTS inputtable2_partition"+str(i))
        cursor.execute("CREATE TABLE inputtable1_partition"+str(i)+" (like "+inputtable1+")")
        cursor.execute("CREATE TABLE inputtable2_partition"+str(i)+" (like "+inputtable2+")")

        insert_query = "INSERT INTO inputtable1_partition"+str(i)+ "( SELECT * FROM "+inputtable1+" where "+ table1joincolumn+ " < "+str(max_value)+" and "+ table1joincolumn +" >= "+str(min_value)+")"
        cursor.execute(insert_query)
        insert_query = "INSERT INTO inputtable2_partition"+str(i)+ "( SELECT * FROM "+inputtable2+" where "+ table2joincolumn+ " < "+str(    max_value)+" and "+ table2joincolumn +" >= "+str(min_value)+")"
        cursor.execute(insert_query)

        openconnection.commit()
        cursor.close()
    except Exception as detail:
        print("problem in range part join insert is --", detail)


def parallel_join(inputtable1, inputtable2, table1joincolumn, table2joincolumn, outputtable, i, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("SELECT "+table1joincolumn+" FROM inputtable1_partition"+str(i))
        column1 = cursor.fetchall()
     
        cursor.execute("SELECT "+table2joincolumn+" FROM inputtable2_partition"+str(i))
        column2 = cursor.fetchall()
    
        cursor.execute("SELECT * FROM inputtable1_partition"+str(i))
        list1 = cursor.fetchall()
     
        cursor.execute("SELECT * FROM inputtable2_partition"+str(i))
        list2 = cursor.fetchall()
    
        for k in range(len(column1)):
            for j in range(len(column2)):
                if(column1[k] == column2[j]):
                    result = list1[k] + list2[j]
                    newquery = "INSERT INTO "+outputtable+" VALUES "+str(result)
                    #print(newquery)
                    cursor.execute(newquery)
        
        openconnection.commit()
        cursor.close()
        delete_partitions_and_exit("inputtable1_partition"+str(i), openconnection)
        delete_partitions_and_exit("inputtable2_partition"+str(i), openconnection)
    except Exception as detail:
        print("problem in parallel_join is -- "+str(i), detail)

###################### connection methods ##############
def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def delete_partitions_and_exit(ratingstablename, openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name LIKE '%_round_par%' OR table_name LIKE '%_range_par%'")
    rows=cur.fetchall()
    cur.execute("DROP TABLE IF EXISTS "+ ratingstablename)
    for i in range(0,len(rows)):
        cur.execute("DROP TABLE IF EXISTS "+ rows[i][0])
    openconnection.commit()
    # Clean up
    cur.close()
    

# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)
        con=getopenconnection()
        #with getopenconnection() as con:
	if con is not None:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
            delete_partitions_and_exit("ratings", con)
            loadratings("ratings", 'ratings.dat', con)
            ParallelSort("ratings", "rating", "outputtable", con)
	    ParallelJoin ("ratings", "ratings", "user_id", "user_id", "outputtablejoin", con)
	    #roundrobininsert("ratings", 1, 1, 1, con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print("OOPS! This is the error ==> " , detail)
