import sys

import pymongo
import pymysql.cursors
from pymysql.constants import CLIENT
from pymongo import MongoClient

client = MongoClient("mongodb://root:root@localhost/", serverSelectionTimeoutMS=1000)
db = client.PCStats
db.main.create_index("timestamp", unique=True)

oldData = list(db.main.find().sort("timestamp", pymongo.DESCENDING).limit(1))
# print(oldData[0]['timestamp'])
# sys.exit(0)

connection = pymysql.connect(
    host="localhost",
    user="PCStats",
    password="PCStatsP4ss",
    database="PCStats",
    cursorclass=pymysql.cursors.DictCursor,
    client_flag=CLIENT.MULTI_STATEMENTS
)
bulkSize = 5000

with connection:
    # with connection.cursor(pymysql.cursors.SSDictCursor) as cursor:
    with connection.cursor() as cursor:
        # select * from data_store limit 5;
        # select * from cpu_cores where timestamp = (select timestamp from data_store limit 1);
        # select * from gpu where timestamp = (select timestamp from data_store limit 1);
        # select * from partitions where timestamp = (select timestamp from data_store limit 1);
        if len(oldData) > 0:
            cursor.execute("select * from data_store where timestamp > %s order by timestamp", (oldData[0]['timestamp'],))
        else:
            cursor.execute("select * from data_store order by timestamp")
        rows = []
        row = cursor.fetchone()
        counter = 0
        while row:
            with connection.cursor() as cpu_cursor:
                cpu_cursor.execute("select `index`, temperature, usage_percent from cpu_cores where timestamp = %s", (row['timestamp'],))
                cpus = cpu_cursor.fetchall()
            with connection.cursor() as gpu_cursor:
                gpu_cursor.execute("select `index`, temperature, ram_usage, ram_max, utilization_percent, fan_percent from gpu where timestamp = %s", (row['timestamp'],))
                gpus = gpu_cursor.fetchall()
            with connection.cursor() as partition_cursor:
                partition_cursor.execute("select path, size, used from partitions where timestamp = %s", (row['timestamp'],))
                partitions = partition_cursor.fetchall()

            row['cpu_cores'] = cpus
            row['gpus'] = gpus
            row['partitions'] = partitions

            row['cpu_package_temperature'] = float(row['cpu_package_temperature'])
            row['load'] = float(row['load'])
            for cpu in row['cpu_cores']:
                cpu['temperature'] = float(cpu['temperature'])
            for gpu in row['gpus']:
                gpu['temperature'] = float(gpu['temperature'])
            # print(row)
            rows.append(row)
            if counter % bulkSize == 0:
                db.main.insert_many(rows)
                rows = []
                print(counter)

            counter += 1
            row = cursor.fetchone()
    if counter % bulkSize != 0:
        db.main.insert_many(rows)
        print(counter)
