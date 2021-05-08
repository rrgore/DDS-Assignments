#
# Assignment2 Interface
#

import psycopg2
import os
import sys
from psycopg2 import sql
import threading
import traceback

DATABASE_NAME = 'dds_assignment2'
RANGE_PART_PREFIX = 'range_part'

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    SortingColumnName = SortingColumnName.lower()

    cur = openconnection.cursor()
            
    minQuery = sql.SQL("select min({col}) from {table}").format(
        col=sql.Identifier(SortingColumnName),
        table=sql.Identifier(InputTable)
    )
    cur.execute(minQuery)
    minVal = cur.fetchone()[0]

    maxQuery = sql.SQL("select max({col}) from {table}").format(
        col=sql.Identifier(SortingColumnName),
        table=sql.Identifier(InputTable)
    )
    cur.execute(maxQuery)
    maxVal = cur.fetchone()[0]

    colRange = maxVal - minVal
    interval_size = colRange/5
    rangeStart = minVal
    threads = list()

    for i in range(5):
        rangeEnd = rangeStart + interval_size
        x = threading.Thread(target=partition_sort, args=(InputTable, SortingColumnName, rangeStart, rangeEnd, i, openconnection))
        threads.append(x)
        x.start()
        rangeStart = rangeEnd

    for x in threads:
        x.join()

    cur.execute(
        sql.SQL("DROP TABLE IF EXISTS {}")
            .format(sql.Identifier(OutputTable))
    )

    table_schema = get_schema(InputTable, cur)
    fields_string = ", ".join(table_schema)
    query = "CREATE TABLE "+OutputTable+" ("+fields_string+");"
    cur.execute(query)

    for i in range(5):
        frag_name = RANGE_PART_PREFIX + str(i)
        query = "INSERT INTO "+OutputTable+" SELECT * FROM "+frag_name
        cur.execute(query)
        
    cur.close()
    openconnection.commit()

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    Table1JoinColumn = Table1JoinColumn.lower()
    Table2JoinColumn = Table2JoinColumn.lower()
    
    cur = openconnection.cursor()
    
    minQuery = sql.SQL("select min({col}) from {table}").format(
        col=sql.Identifier(Table1JoinColumn),
        table=sql.Identifier(InputTable1)
    )
    cur.execute(minQuery)
    minVal = cur.fetchone()[0]

    maxQuery = sql.SQL("select max({col}) from {table}").format(
        col=sql.Identifier(Table1JoinColumn),
        table=sql.Identifier(InputTable1)
    )
    cur.execute(maxQuery)
    maxVal = cur.fetchone()[0]

    colRange = maxVal - minVal
    interval_size = colRange/5
    rangeStart = minVal
    threads = list()

    for i in range(5):
        rangeEnd = rangeStart + interval_size
        x = threading.Thread(target=partition_join, args=(InputTable1, Table1JoinColumn, InputTable2, Table2JoinColumn, rangeStart, rangeEnd, i, openconnection))
        threads.append(x)
        x.start()
        rangeStart = rangeEnd

    for x in threads:
        x.join()
        
    cur.execute("DROP TABLE IF EXISTS "+OutputTable)
        
    join_col_infos = get_schema('range_part0', cur)
    join_col_string = ", ".join(join_col_infos)
    
    query = "CREATE TABLE "+OutputTable+" ("+join_col_string+");"
    cur.execute(query)

    for i in range(5):
        frag_name = RANGE_PART_PREFIX + str(i)
        query = "INSERT INTO "+OutputTable+" SELECT * FROM "+frag_name
        cur.execute(query)
        
    cur.close()
    openconnection.commit()

def partition_sort(input_table, colname, rangeStart, rangeEnd, frag_num, conn):
    cursor = conn.cursor()
    
    query1 = sql.SQL("select * from {table} where {col} between %s and %s order by {col} asc").format(
        col=sql.Identifier(colname),
        table=sql.Identifier(input_table))
    cursor.execute(query1, (rangeStart, rangeEnd))
    query_op = cursor.fetchall()

    frag_name = RANGE_PART_PREFIX + str(frag_num)
    cursor.execute(
        sql.SQL("DROP TABLE IF EXISTS {}")
            .format(sql.Identifier(frag_name))
    )
    
    table_schema = get_schema(input_table, cursor)
    fields_string = ", ".join(table_schema)
    query = "CREATE TABLE "+frag_name+" ("+fields_string+");"
    cursor.execute(query)
    
    placeholders_list = ["%s" for i in table_schema]
    placeholders_string = ", ".join(placeholders_list)
    for record in query_op:
        query = "INSERT INTO "+frag_name+" VALUES ("+placeholders_string+")"
        val_list = [val for val in record]
        cursor.execute(query, val_list)
            
    cursor.close()
    conn.commit()

def partition_join(InputTable1, Table1JoinColumn, InputTable2, Table2JoinColumn, rangeStart, rangeEnd, frag_num, openconnection):
    cursor = openconnection.cursor()

    query1 = sql.SQL("select * from {table1}, {table2} where {table1}.{col1}={table2}.{col2} and {table1}.{col1} between %s and %s order by {table1}.{col1} asc").format(
        col1=sql.Identifier(Table1JoinColumn),
        table1=sql.Identifier(InputTable1),
        col2=sql.Identifier(Table2JoinColumn),
        table2=sql.Identifier(InputTable2))
    cursor.execute(query1, (rangeStart, rangeEnd))
    query_op = cursor.fetchall()        

    """
    (1) Get the schema of both tables
    (2) Create new table using both columns
    """
    col_infos = []
    table1_schema = get_schema(InputTable1, cursor)
    col_infos.extend(table1_schema)
    table2_schema = get_schema(InputTable2, cursor)
    col_infos.extend(table2_schema)
        
    frag_name = RANGE_PART_PREFIX + str(frag_num)
    fields_string = ", ".join(col_infos)
    query = "CREATE TABLE "+frag_name+" ("+fields_string+");"
    cursor.execute(query)

    placeholders_list = ["%s" for i in col_infos]
    placeholders_string = ", ".join(placeholders_list)
    for record in query_op:
        query = "INSERT INTO "+frag_name+" VALUES ("+placeholders_string+")"
        val_list = [val for val in record]
        cursor.execute(query, val_list)

    cursor.close()
    openconnection.commit()

def get_schema(table_name, cursor):
    try:
        query = sql.SQL("""                              
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s;
            """)
        cursor.execute(query, (table_name,))
        schema = cursor.fetchall()
        
#         print(schema)
        sql_schema = list()
        for col in schema:
            if col[1] == 'double precision' or col[1] == 'real':
                col_info = "{} real".format(col[0])
            elif col[1] == 'character varying':
                col_info = "{} varchar(100)".format(col[0])
            else:
                col_info = "{} integer".format(col[0])
            sql_schema.append(col_info)
            
        return sql_schema
        
    except:
        traceback.print_exc()


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='dds_assignment2'):
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
    con.commit()
    con.close()

# Donot change this function
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
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


