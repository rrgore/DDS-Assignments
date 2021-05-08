import psycopg2
import os
import sys
import math
from psycopg2 import sql

RANGE_TABLE_PREFIX = 'range_ratings_part'
RROBIN_TABLE_PREFIX = 'round_robin_ratings_part'


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    with openconnection:
        with openconnection.cursor() as cur:
            # Drop table if it exists
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {}")
                    .format(sql.Identifier(ratingstablename))
            )
            
            # Create a table
            cur.execute(
                sql.SQL("CREATE TABLE {} (userid integer, empty1 varchar(10), movieid integer, empty2 varchar(10), rating float, empty3 varchar(10), timestamp integer)")
                    .format(sql.Identifier(ratingstablename))
            )
            
            # Copy data from file to table
            with open(ratingsfilepath) as f:
                cur.copy_from(
                    f, ratingstablename, ':', columns=('userid', 'empty1', 'movieid', 'empty2', 'rating', 'empty3', 'timestamp')
                )
            
            cur.execute(
                sql.SQL("ALTER TABLE {} DROP COLUMN empty1, DROP COLUMN empty2, DROP COLUMN empty3, DROP COLUMN timestamp")
                    .format(sql.Identifier(ratingstablename))
            )
            
            # Create metadata table
            cur.execute("DROP TABLE IF EXISTS ratings_metadata")
            cur.execute("CREATE TABLE ratings_metadata (description varchar(100), amt integer)")


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    # Find out interval size of ratings in fragmented tables
    if numberofpartitions == 0:
        return
    interval_size = 5.0/numberofpartitions
    
    # Determine the intervals
    # Range is of type low < x <= high
    rangeStart = 0
    nameIx = 0
    
    # For each interval, find values from DB
    with openconnection:
        with openconnection.cursor() as cur:
            while nameIx < numberofpartitions:
                rangeEnd = rangeStart+interval_size
                if int(rangeStart) == 0:
                    cur.execute(
                        # Since first i.e. 0 gets excluded
                        sql.SQL("SELECT * FROM {} WHERE rating<=%s")
                            .format(sql.Identifier(ratingstablename)), 
                        (rangeEnd,)
                    )
                else:
                    cur.execute(
                        sql.SQL("SELECT * FROM {} WHERE rating>%s AND rating<=%s")
                            .format(sql.Identifier(ratingstablename)), 
                        (rangeStart, rangeEnd)
                    )
                query_op = cur.fetchall()               
        
                # Create new table for fragment, push these values in it
                frag_name = RANGE_TABLE_PREFIX+str(nameIx)
                
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {}")
                        .format(sql.Identifier(frag_name))
                )
            
                cur.execute(
                    sql.SQL("CREATE TABLE {} (userid integer, movieid integer, rating float)")
                        .format(sql.Identifier(frag_name))
                )
                
                for (userid, movieid, rating) in query_op:
                    cur.execute(
                        sql.SQL("INSERT INTO {} (userid, movieid, rating) VALUES (%s, %s, %s)")
                            .format(sql.Identifier(frag_name)),
                        (userid, movieid, rating)
                    )
                rangeStart = rangeEnd
                nameIx += 1
            
            # Insert into metadata table
            cur.execute("INSERT INTO ratings_metadata (description, amt) VALUES (%s, %s)",
                ("Range partitions", numberofpartitions)
            )


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    # Generate N tables
    with openconnection:
        with openconnection.cursor() as cur:
            for i in range(0, numberofpartitions):
                frag_name = RROBIN_TABLE_PREFIX+str(i)
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {}")
                        .format(sql.Identifier(frag_name))
                )

                cur.execute(
                    sql.SQL("CREATE TABLE {} (userid integer, movieid integer, rating float)")
                        .format(sql.Identifier(frag_name))
                )
                
            cur.execute(
                sql.SQL("SELECT * FROM {}")
                    .format(sql.Identifier(ratingstablename))
            )
            query_op = cur.fetchall()
            nameIx = 0
            
            # Insert tuples in tables in round robin manner
            for (userid, movieid, rating) in query_op:
                frag_name = RROBIN_TABLE_PREFIX+str(nameIx)
                cur.execute(
                    sql.SQL("INSERT INTO {} (userid, movieid, rating) VALUES (%s, %s, %s)")
                        .format(sql.Identifier(frag_name)),
                    (userid, movieid, rating)
                )
                nameIx += 1
                nameIx = nameIx%numberofpartitions
                
            # Insert into metadata
            cur.execute("INSERT INTO ratings_metadata (description, amt) VALUES (%s, %s)",
                ("Round robin partitions", numberofpartitions)
            )


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    # First insert to table
    with openconnection:
        with openconnection.cursor() as cur:
            cur.execute(
                sql.SQL("INSERT INTO {} (userid, movieid, rating) VALUES (%s, %s, %s)")
                    .format(sql.Identifier(ratingstablename)),
                (userid, itemid, rating)
            )
            
            # Find number of fragments
            metadata_name = "ratings_metadata"
            cur.execute(
                sql.SQL("SELECT amt FROM {} WHERE description=%s")
                    .format(sql.Identifier(metadata_name)),
                ('Round robin partitions',)
            )
            query_op = cur.fetchall()
            if not query_op or not query_op[0]:
                return
            num_fragments = query_op[0][0]
            
            # Find correct fragment
            cur.execute(
                sql.SQL("SELECT COUNT(userid) FROM {}")
                    .format(sql.Identifier(ratingstablename))
            )
            query_op = cur.fetchall()
            if not query_op or not query_op[0]:
                return
            num_records = query_op[0][0]
            if num_records%num_fragments == 0:
                frag_id = num_fragments - 1
            else:
                frag_id = num_records%num_fragments - 1
            frag_name = RROBIN_TABLE_PREFIX + str(frag_id)
        
            # Insert to fragment
            cur.execute(
                sql.SQL("INSERT INTO {} (userid, movieid, rating) VALUES (%s, %s, %s)")
                    .format(sql.Identifier(frag_name)),
                (userid, itemid, rating)
            )


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    with openconnection:
        with openconnection.cursor() as cur:
            cur.execute(
                sql.SQL("INSERT INTO {} (userid, movieid, rating) VALUES (%s, %s, %s)")
                    .format(sql.Identifier(ratingstablename)),
                (userid, itemid, rating)
            )
            
            # Find number of fragments
            metadata_name = "ratings_metadata"
            cur.execute(
                sql.SQL("SELECT amt FROM {} WHERE description=%s")
                    .format(sql.Identifier(metadata_name)),
                ('Range partitions',)
            )
            query_op = cur.fetchall()
            if not query_op or not query_op[0]:
                return
            num_fragments = query_op[0][0]
            if num_fragments == 0:
                return

            # Find appropriate frag num
            interval_size = 5.0/num_fragments
            if rating == 0:
                frag_num = 0
            else:
                frag_num = math.ceil(rating/interval_size)-1

            frag_name = RANGE_TABLE_PREFIX + str(frag_num)
        
            # Insert to fragment
            cur.execute(
                sql.SQL("INSERT INTO {} (userid, movieid, rating) VALUES (%s, %s, %s)")
                    .format(sql.Identifier(frag_name)),
                (userid, itemid, rating)
            )


def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    if os.path.exists(outputPath):
        os.remove(outputPath)        
    
    with openconnection:
        with openconnection.cursor() as cur:
            # Get fragments
            metadata_name = "ratings_metadata"
            cur.execute(
                sql.SQL("SELECT amt FROM {} WHERE description=%s")
                    .format(sql.Identifier(metadata_name)),
                ('Range partitions',)
            )
            query_op = cur.fetchall()
            if query_op and query_op[0] and query_op[0][0] != 0:
                num_fragments = query_op[0][0]
                
                # Check if fragment is relevant
                relevant_map = [0 for i in range(num_fragments)]
                interval_size = 5.0/num_fragments
                curr_min = 0.0
                i = 0
                while i < num_fragments:
                    # allowed values in fragment: min<=r<max
                    curr_max = curr_min+interval_size
                    if curr_min<=ratingMaxValue and curr_max>ratingMinValue:
                        relevant_map[i] = 1
                    i += 1
                    curr_min = curr_max
                
                # Query each relevant fragment
                for i in range(num_fragments):
                    if relevant_map[i] == 1:
                        frag_name = RANGE_TABLE_PREFIX+str(i)
                        cur.execute(
                            sql.SQL("SELECT * FROM {} WHERE rating>=%s AND rating<=%s")
                                .format(sql.Identifier(frag_name)),
                            (ratingMinValue,ratingMaxValue)
                        )
                        query_op = cur.fetchall()
                        
                        # Write to file path
                        with open(outputPath,"a+") as f:
                            for (userid, movieid, rating) in query_op:
                                f.write(
                                    "{0},{1},{2},{3}\n".format(frag_name, userid, movieid, rating)
                                )
                         
            # Get round robin fragments
            cur.execute(
                sql.SQL("SELECT amt FROM {} WHERE description=%s")
                    .format(sql.Identifier(metadata_name)),
                ('Round robin partitions',)
            )
            query_op = cur.fetchall()
            if not query_op or not query_op[0]:
                return                
            num_fragments = query_op[0][0]
            
            for i in range(num_fragments):
                frag_name = RROBIN_TABLE_PREFIX+str(i)
                cur.execute(
                    sql.SQL("SELECT * FROM {} WHERE rating>=%s AND rating<=%s")
                        .format(sql.Identifier(frag_name)),
                    (ratingMinValue,ratingMaxValue)
                )
                query_op = cur.fetchall()

                # Write to file path
                with open(outputPath,"a+") as f:
                    for (userid, movieid, rating) in query_op:
                        f.write(
                            "{0},{1},{2},{3}\n".format(frag_name, userid, movieid, rating)
                        )


def pointQuery(ratingValue, openconnection, outputPath):
    if os.path.exists(outputPath):
        os.remove(outputPath)
        
    with openconnection:
        with openconnection.cursor() as cur:
            # Get range fragments
            metadata_name = "ratings_metadata"
            cur.execute(
                sql.SQL("SELECT amt FROM {} WHERE description=%s")
                    .format(sql.Identifier(metadata_name)),
                ('Range partitions',)
            )
            query_op = cur.fetchall()
            if query_op and query_op[0] and query_op[0][0] != 0:
                num_fragments = query_op[0][0]
                
                # Find appropriate frag num
                interval_size = 5.0/num_fragments
                relevant_frag = -1
                if ratingValue == 0:
                    relevant_frag = 0
                else:
                    relevant_frag = math.ceil(ratingValue/interval_size)-1
                    
                frag_name = RANGE_TABLE_PREFIX + str(relevant_frag)
                cur.execute(
                    sql.SQL("SELECT * FROM {} WHERE rating=%s")
                        .format(sql.Identifier(frag_name)),
                    (ratingValue,)
                )
                query_op = cur.fetchall()

                # Write to file path
                with open(outputPath,"a+") as f:
                    for (userid, movieid, rating) in query_op:
                        f.write(
                            "{0},{1},{2},{3}\n".format(frag_name, userid, movieid, rating)
                        )
                
            # Get round robin fragments
            cur.execute(
                sql.SQL("SELECT amt FROM {} WHERE description=%s")
                    .format(sql.Identifier(metadata_name)),
                ('Round robin partitions',)
            )
            query_op = cur.fetchall()
            if not query_op or not query_op[0]:
                return
            num_fragments = query_op[0][0]
            
            for i in range(num_fragments):
                frag_name = RROBIN_TABLE_PREFIX+str(i)
                cur.execute(
                    sql.SQL("SELECT * FROM {} WHERE rating=%s")
                        .format(sql.Identifier(frag_name)),
                    (ratingValue,)
                )
                query_op = cur.fetchall()

                # Write to file path
                with open(outputPath,"a+") as f:
                    for (userid, movieid, rating) in query_op:
                        f.write(
                            "{0},{1},{2},{3}\n".format(frag_name, userid, movieid, rating)
                        )


def createDB(dbname='dds_assignment1'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
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

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
