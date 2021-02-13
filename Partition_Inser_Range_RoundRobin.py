import psycopg2
import math

def getOpenConnection(user='postgres', password='postgre@123', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# To load the data from csv to databases
def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cursor = openconnection.cursor()
    cursor.execute('''
    CREATE TABLE {}
    (
        userid integer NOT NULL,
        temp1 char,
        movieid integer NOT NULL,
        temp2 char,
        rating float NOT NULL,
        temp3 char,
        timestamp bigint,   
        CONSTRAINT ratings2_userid_movieid PRIMARY KEY (userid, movieid)
    );

    '''.format(ratingstablename))
    with open(ratingsfilepath, 'r') as fileratings:
        cursor.copy_from(fileratings, ratingstablename, sep=':')

    cursor.execute('''
    ALTER TABLE {} 
    DROP COLUMN IF EXISTS temp1,
    DROP COLUMN IF EXISTS temp2,
    DROP COLUMN IF EXISTS temp3,
    DROP COLUMN IF EXISTS timestamp
    ;
    '''.format(ratingstablename))
    openconnection.commit()
    cursor.close()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    ratings_range = round(5.0 / numberofpartitions, 2)
    lower_range = 0
    upper_range = ratings_range
    cursor = openconnection.cursor()
    for i in range(numberofpartitions):
        if i == 0:
            cursor.execute('''
            CREATE TABLE range_part{} AS
            SELECT * from {} where rating >= {} AND rating <= {};            
            '''.format(i, ratingstablename, lower_range, upper_range))
        else:
            cursor.execute('''
            CREATE TABLE range_part{} AS
            SELECT * from {} where rating > {} AND rating <= {};            
            '''.format(i, ratingstablename, lower_range, upper_range))
        lower_range = upper_range
        upper_range += ratings_range
    openconnection.commit()
    cursor.close()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cursor = openconnection.cursor()
    cursor.execute("select * from {} ;".format(ratingstablename))
    rows = cursor.fetchall()
    insert_array = [[] for _ in range(numberofpartitions)]
    for i in range(cursor.rowcount):
        array_select = i % numberofpartitions
        insert_array[array_select].append(rows[i])

    for i in range(numberofpartitions):
        cursor.execute("""CREATE TABLE rrobin_part{} AS
                       SELECT u.userid, u.movieid, u.rating
                       FROM unnest(%s) u(userid integer, movieid integer, rating numeric)""".format(i),
                       (insert_array[i],))
    openconnection.commit()
    cursor.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cursor = openconnection.cursor()
    cursor.execute('''SELECT count(*) FROM information_schema.tables 
        WHERE table_name LIKE 'rrobin_part%' AND table_type = 'BASE TABLE'
        ''')
    numberofpartitions = cursor.fetchall()[0][0]
    countofrows = []
    for i in range(numberofpartitions):
        cursor.execute(''' SELECT COUNT(*) FROM rrobin_part{}
        '''.format(i))
        countofrows.append(cursor.fetchall()[0][0])
    inserted = 0
    for i in range(numberofpartitions - 1):
        if countofrows[i] > countofrows[i + 1]:
            cursor.execute(''' INSERT INTO rrobin_part{} (Userid, movieid, rating) 
               VALUES ({},{},{});
               '''.format(i + 1, userid, itemid, rating))
            inserted = 1
    if inserted == 0:
        cursor.execute(''' INSERT INTO rrobin_part{} (Userid, movieid, rating) 
                  VALUES ({},{},{});
                  '''.format(0, userid, itemid, rating))
    openconnection.commit()
    cursor.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cursor = openconnection.cursor()
    cursor.execute('''SELECT count(*) FROM information_schema.tables 
    WHERE table_name LIKE 'range_part%' AND table_type = 'BASE TABLE'
    ''')
    numberofpartitions = cursor.fetchall()[0][0]
    range = round(5.0 / numberofpartitions, 2)
    partition = math.ceil(rating / range)
    if partition > 0:
        partition -= 1
    cursor.execute(''' INSERT INTO range_part{} (Userid, movieid, rating) 
    VALUES ({},{},{});
    '''.format(int(partition), userid, itemid, rating))
    openconnection.commit()
    cursor.close()


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
    con.close()


def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()


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
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()
