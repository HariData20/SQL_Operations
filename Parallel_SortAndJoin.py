#!/usr/bin/python2.7
#
# Assignment3 Interface
#


import psycopg2
import os
import sys

def RangePartition(srcTable, columnName, numberofpartitions, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("select * from information_schema.tables where table_name='%s'" % srcTable)
        if not bool(cursor.rowcount):
            print "Please Load {} Table first!!!".format(srcTable)
            return
        cursor.execute("DROP TABLE IF EXISTS %sMetadata" % (srcTable))
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS %sMetadata(PartitionNum INT, lower_range REAL, upper_range REAL)" % (srcTable))
        cursor.execute("select min(%s) from %s" % (columnName, srcTable))

        lower = cursor.fetchall()[0][0]
        cursor.execute("select max(%s) from %s" % (columnName, srcTable))
        upper = cursor.fetchall()[0][0]

        step = (upper - lower) / (float)(numberofpartitions)
        i = 0;
        while lower < upper:
            lowerLimit = lower
            upperLimit = lower + step
            if lowerLimit < 0:
                lowerLimit = 0.0
            cursor.execute(
                "INSERT INTO %sMetadata (PartitionNum, lower_range, upper_range) VALUES(%d, %f, %f)" % (
                    srcTable, i, lowerLimit, upperLimit))
            lower = upperLimit
            i += 1;
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
        return srcTable + 'Metadata'

def Insert_Sorted(srcTable, index, lower, upper, sortColumn, openconnection):
    try:
        cursor = openconnection.cursor()
        newTable = srcTable+str(index)
        if index == 0:
            cursor.execute("DROP TABLE IF EXISTS %s;" % (newTable))
            query = "CREATE TABLE %s AS SELECT * FROM %s WHERE %s >= %s AND %s <= %s ORDER BY %s" % (
                    newTable, srcTable, sortColumn, lower, sortColumn, upper, sortColumn)
            cursor.execute(query)
        else:
            cursor.execute("DROP TABLE IF EXISTS %s;" % (newTable))
            query = "CREATE TABLE %s AS SELECT * FROM %s WHERE %s > %s AND %s <= %s ORDER BY %s" % (
                newTable, srcTable, sortColumn, lower, sortColumn, upper, sortColumn)
            cursor.execute(query)
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

def Join_Sorted(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    try:
        cursor = openconnection.cursor()

        if Table1JoinColumn == Table2JoinColumn:
            cursor.execute("CREATE TABLE %s AS SELECT * FROM %s JOIN %s USING (%s); " % (
                OutputTable, InputTable1, InputTable2, Table1JoinColumn))
        else:
            cursor.execute("CREATE TABLE %s AS SELECT * FROM %s,%s where %s.%s = %s.%s; " % (
            OutputTable, InputTable1, InputTable2, InputTable1, Table1JoinColumn, InputTable2, Table2JoinColumn ))
        openconnection.commit()

    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
import threading
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort(InputTable, SortingColumnName, OutputTable, openconnection):
    numberofpartitions = 5
    cursor = openconnection.cursor()
    metadataTable = RangePartition(InputTable, SortingColumnName, numberofpartitions, openconnection)
    cursor.execute("SELECT * from %s ;" % (metadataTable))
    partition_info = cursor.fetchall()
    threads = []
    for partition in partition_info:
        t = threading.Thread(target=Insert_Sorted, args=(
            InputTable, partition[0], partition[1], partition[2],SortingColumnName, openconnection,))
        threads.append(t)
        t.start()
    cursor.execute("DROP TABLE IF EXISTS %s;" % (OutputTable))
    cursor.execute("CREATE TABLE %s AS TABLE %s WITH NO DATA;" % (OutputTable, InputTable))
    for partition in partition_info:
        cursor.execute("INSERT INTO %s SELECT * from %s ;" % (OutputTable, InputTable + str(partition[0])))
    openconnection.commit()
    cursor.close()

def ParallelJoin(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    numberofpartitions = 5
    cursor = openconnection.cursor()
    metadataTable = RangePartition(InputTable1, Table1JoinColumn, numberofpartitions, openconnection)
    cursor.execute("SELECT * from %s ;" % (metadataTable))
    partition_info = cursor.fetchall()
    threads = []
    for partition in partition_info:
        Insert_Sorted(InputTable1, partition[0], partition[1], partition[2], Table1JoinColumn, openconnection)
        Insert_Sorted(InputTable2, partition[0], partition[1], partition[2], Table2JoinColumn, openconnection)
    for partition in partition_info:
        t = threading.Thread(target=Join_Sorted, args=(
            InputTable1+str(partition[0]), InputTable2+str(partition[0]), Table1JoinColumn, Table2JoinColumn,
            OutputTable+str(partition[0]), openconnection,))
        threads.append(t)
        t.start()
    cursor.execute("DROP TABLE IF EXISTS %s;" % (OutputTable))

    if Table1JoinColumn == Table2JoinColumn:
        cursor.execute("CREATE TABLE %s AS SELECT * FROM %s JOIN %s USING (%s) WITH NO DATA; " % (
            OutputTable, InputTable1, InputTable2, Table1JoinColumn))
    else:
        cursor.execute("CREATE TABLE %s AS SELECT * FROM %s,%s WITH NO DATA;" % (OutputTable, InputTable1, InputTable2))
    for partition in partition_info:
        cursor.execute("INSERT INTO %s SELECT * from %s ;" % (OutputTable, OutputTable + str(partition[0])))
    openconnection.commit()
    cursor.close()


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


# Donot change this function
def createDB(dbname='dds_assignment'):
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
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()
