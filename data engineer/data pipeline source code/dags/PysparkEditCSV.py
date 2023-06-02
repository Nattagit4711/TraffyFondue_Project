import pandas as pd
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
import findspark


def csvedit():
    findspark.init()
    spark_url = 'local'
    spark = SparkSession.builder\
        .master(spark_url)\
        .appName('Spark SQL')\
        .getOrCreate()
    f= open("/Users/guyrawit/data_project/checkpoint.txt", "r")
    filename = f.readline()
    
    path = '/Users/guyrawit/data_project/{}'.format(filename)   # path to read
    df = spark.read.option("delimiter", ",").option("header", True).csv(path)
    # df2 = df.select(df['message_id'], df['type'],
    #                 df['comment'], df['coords'], df['photo'])

    # df2 = df2.filter((df2['type'] == 'ถนน') | (df2['type'] == 'ทางเท้า') | (df2['type'] == 'น้ำท่วม') | (df2['type'] == 'เเสงสว่าง') | (df2['type'] == 'ความสะอาด') | (df2['type'] == 'ท่อระบายน้ำ')
    #                  | (df2['type'] == 'กีดขวาง') | (df2['type'] == 'สายไฟ') | (df2['type'] == 'จราจร') | (df2['type'] == 'สะพาน') | (df2['type'] == 'ความปลอดภัย') | (df2['type'] == 'ต้นไม้') | (df2['type'] == 'คลอง')
    #                  | (df2['type'] == 'สัตว์จรจัด'))
    # df2 = df2.filter("photo is not NULL")
    df = df.filter(df.type.isNull())
    df.coalesce(1).write.option("header", "true").csv("/Users/guyrawit/data_project/{}-spark".format(filename[:-4]))
    print("{}-spark has been created !!".format(filename[:-4]))
    f = open("/Users/guyrawit/data_project/filename.txt", "w")
    f.write("/Users/guyrawit/data_project/{}-spark".format(filename[:-4]))
    return 0 


