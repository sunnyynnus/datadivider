#!/usr/bin/python3
#
# Interface for the assignement
#
import os
import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    #pass
    cur = openconnection.cursor()
    try:
        #drop table if exists
        cur.execute("DROP TABLE IF EXISTS "+ ratingstablename)

        #creating table
        cur.execute("CREATE TABLE "+ ratingstablename +" (p_id serial primary key, user_id integer, delim_temp_1 char(2), movie_id integer, delim_temp_2 char(2), rating real, delim_temp_3 char(2), timestamp interval)")
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
        print("problem in load is --")


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    #pass
    try:
        cur = openconnection.cursor()
        #creating metadata table
        cur.execute("DROP TABLE IF EXISTS metadata_range_par")
        cur.execute("CREATE TABLE metadata_range_par(table_name char(50), rating_start decimal, rating_end decimal)")
        range_s=0
        range_interval=float(5)/numberofpartitions
        range_e=range_interval

        for i in range(numberofpartitions):
                tablename=ratingstablename+"_range_par_"+ str(i)
                cur.execute("DROP TABLE IF EXISTS "+tablename)

                prepareQuery="CREATE TABLE "+tablename +" (like "+ratingstablename+")"
                cur.execute(prepareQuery)
                metadataQuery="INSERT INTO metadata_range_par VALUES('"+tablename+"',"+str(range_s)+","+str(range_e)+")"
                cur.execute(metadataQuery)
                #print(metadataQuery)
                #workaround- delete p_id and re-insert
                cur.execute("ALTER TABLE "+ratingstablename+"_range_par_"+str(i)+" DROP p_id")
                #cur.execute("ALTER TABLE "+ratingstablename+"_range_par_"+str(i)+" ADD p_id SERIAL PRIMARY KEY")

                rangeQuery="INSERT INTO "+tablename+" (user_id, movie_id, rating, timestamp) SELECT user_id, movie_id, rating, timestamp FROM "+ratingstablename+" WHERE rating > "+str(range_s)+" AND rating <= "+str(range_e)
                cur.execute(rangeQuery)

                range_s=range_e
                range_e+=range_interval
        openconnection.commit()
        cur.close()

        print("*******Range partitioning Completed**********")
    except Exception as detail:
        print("problem in range partition is --")


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    #pass
    try:
        cur=openconnection.cursor()
        cur.execute("DROP TABLE IF EXISTS metadata_round_par")
        cur.execute("CREATE TABLE metadata_round_par(p_id serial primary key, table_name char(50), flag smallint)")

        flag=0
        cur.execute("SELECT count(*) FROM "+ ratingstablename)
        rows=cur.fetchall()
        count=rows[0][0]
        for i in range(numberofpartitions):
                tablename=ratingstablename+"_round_par_"+ str(i)
                cur.execute("DROP TABLE IF EXISTS "+tablename)

                query="CREATE TABLE "+tablename+" (like "+ratingstablename+")"
                cur.execute(query)
                cur.execute("ALTER TABLE "+tablename+" DROP p_id")
                # make flag=1 for the last record
                if(i == (count % numberofpartitions)):
                        flag=1
                metadataquery="INSERT INTO metadata_round_par(table_name, flag) VALUES('"+tablename+"', "+str(flag)+")"
                cur.execute(metadataquery)
                #insert code
                insertquery="INSERT INTO "+tablename+" (user_id, movie_id, rating, timestamp) SELECT user_id, movie_id, rating, timestamp FROM "+ratingstablename+"  WHERE " + str(i) +" = (p_id % "+str(numberofpartitions)+")"
                cur.execute(insertquery)
                flag=0
        openconnection.commit()
        cur.close()

        print("***********Round robin partitioning Completed********")
    except Exception as detail:
        print("problem in RR partition is --")




def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    #pass
    try:
        cur=openconnection.cursor()
        cur.execute("SELECT count(*) FROM metadata_round_par")
        rows=cur.fetchall()
        count=rows[0][0]

        cur.execute("SELECT p_id FROM metadata_round_par WHERE flag=1")
        rows=cur.fetchall()
        index=rows[0][0]
        next_table_offset=index % count
        #reset flag
        cur.execute("UPDATE metadata_round_par SET flag=0 WHERE flag=1")
        des_tablename=ratingstablename+"_round_par_"+ str(next_table_offset)

        insertQuery="INSERT INTO "+des_tablename+" VALUES ("+str(userid)+","+str(itemid)+","+str(rating)+")"
        #print(insertQuery)
        cur.execute(insertQuery)
        #update appropriate flag
        cur.execute("UPDATE metadata_round_par SET flag=1 where table_name = '"+des_tablename+"'")

        openconnection.commit()
        cur.close()
        print("****** Round robin insertion Completed*******")

    except Exception as detail:
        print("problem in RR insertion is --")



def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    #pass
    try:
        cur=openconnection.cursor()

        selectquery="SELECT DISTINCT(table_name) from metadata_range_par WHERE ("+str(rating)+" >=rating_start) and ("+str(rating)+" <rating_end)"
        cur.execute(selectquery)
        rows=cur.fetchall()
        table_name=rows[0][0]
        insertquery="INSERT INTO "+str(table_name)+" VALUES ("+str(userid)+","+str(itemid)+","+str(rating)+")"
        #print(insertquery)
        cur.execute(insertquery)
        openconnection.commit()
        cur.close()

        print("********Range Insertion Completed****")
    except Exception as detail:
        print("problem in rangeinsert is --" )



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
	    rangepartition("ratings", 10, con)
	    rangeinsert("ratings", 4, 4, 4, con)
	    roundrobinpartition("ratings", 5, con)
	    roundrobininsert("ratings", 1, 1, 1, con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print("OOPS! This is the error ==> ")
