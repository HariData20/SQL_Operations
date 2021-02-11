import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
RANGE_METADATA_TABLE = 'RangeRatingsMetadata'
RROBIN_METADATA_TABLE = 'RoundRobinRatingsMetadata'
RANGE_TABLE_PREFIX = 'RangeRatingsPart'
RROBIN_TABLE_PREFIX = 'RoundRobinRatingsPart'

def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    cursor = openconnection.cursor()
    cursor.execute('''
    SELECT * FROM {};
    '''.format(RANGE_METADATA_TABLE))
    partition_info = cursor.fetchall()
    rows = []
    for partition in partition_info:
        if (ratingMinValue <= partition[1] and partition[1] <= ratingMaxValue) or (ratingMinValue <= partition[2] and partition[2] <= ratingMaxValue):
            cursor.execute(
            "SELECT 'RangeRatingsPart{}' as part, * FROM RangeRatingsPart{} where rating BETWEEN {} AND {};".format(
            partition[0], partition[0], ratingMinValue, ratingMaxValue))
            rows.extend(cursor.fetchall())

    cursor.execute('''
    SELECT partitionnum FROM {};
    '''.format(RROBIN_METADATA_TABLE))
    partition_info = cursor.fetchall()[0][0]
    for partition in range(partition_info):
        cursor.execute(
            "SELECT 'RoundRobinRatingsPart{}' as part, * FROM RoundRobinRatingsPart{} where rating BETWEEN {} AND {};".format(
                partition, partition, ratingMinValue, ratingMaxValue))
        rows.extend(cursor.fetchall())
    cursor.close()
    writeToFile('RangeQueryOut.txt', rows)


def PointQuery(ratingsTableName, ratingValue, openconnection):
    cursor = openconnection.cursor()
    cursor.execute('''
    SELECT * FROM {};
    '''.format(RANGE_METADATA_TABLE))
    partition_info = cursor.fetchall()
    rows = []
    for partition in partition_info:
        if partition[0] == 0:
            if partition[1] <= ratingValue and partition[2] >= ratingValue:
                cursor.execute(
                    "SELECT 'RangeRatingsPart{}' as part, * FROM RangeRatingsPart{} where rating = {};".format(
                        partition[0], partition[0], ratingValue))
                rows.extend(cursor.fetchall())
        else:
            if partition[1] < ratingValue and partition[2] >= ratingValue:
                cursor.execute(
                    "SELECT 'RangeRatingsPart{}' as part, * FROM RangeRatingsPart{} where rating = {};".format(
                        partition[0], partition[0], ratingValue))
                rows.extend(cursor.fetchall())
    cursor.execute('''
    SELECT partitionnum FROM {};
    '''.format(RROBIN_METADATA_TABLE))
    partition_info = cursor.fetchall()[0][0]
    for partition in range(partition_info):
        cursor.execute(
            "SELECT 'RoundRobinRatingsPart{}' as part, * FROM RoundRobinRatingsPart{} where rating = {};".format(
                partition, partition, ratingValue))
        rows.extend(cursor.fetchall())

    cursor.close()
    writeToFile('PointQueryOut.txt', rows)
def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()