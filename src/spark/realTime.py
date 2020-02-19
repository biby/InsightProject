import pyspark
import json
from pyspark import SparkContext, SparkConf,SQLContext
import pyspark
from zipcode import zipCode,zipCodeTree
import time
import os
import boto3
import PIL
from PIL import Image
import tempfile
import numpy as np
from itertools import islice
from satelliteImage import satelliteImage
from SparktomySQL import MySQLConnector
from imageProcessor import ImageProcessor
import pyspark.sql.functions
from solarPanels import SolarPanels
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType, LongType

def decode(x):
    dic = json.loads(x[0].decode('utf8').replace("'",'"'))
    l = [dic[key] for key in DataColumns]
    return ','.join(list(map(str,l)))

def processBatchEncloser(data):
    global nbThread
    imP = ImageProcessor(zipsearch,nbThread)
    return imP.processBatch(data)

def fetchzipcodes():
    zipcodes = sc.textFile('s3a://ideanalytics/zcta2010.csv')
    zc =zipcodes.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)
    return zc.collect()

if __name__=='__main__':
    
    #Configuring spark
    conf = SparkConf().setAppName('test')
    sc = SparkContext(conf = conf, pyFiles=['zipcode.py', 'solarirradiance.py','satelliteImage.py','SparktomySQL.py','imageProcessor.py','solarPanels.py','SolarInsight.py'])
    sqlContext = SQLContext(sc)

    #Set up S3 access keys for spark
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.environ["AWS_ACCESS_KEY"])
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ["AWS_SECRET_KEY"])

    #Create a mysqlclient
    sqlhost='10.0.0.8'
    sqluser='ubuntu'
    sqlpassword = os.environ["MYSQL_SECRET_KEY"]
    sqldatabase='zipcloud'
    mysqlConnector = MySQLConnector(sqlContext,host=sqlhost, user=sqluser, password=sqlpassword,databaseName=sqldatabase)
    DFRow = Row("id","zipcode","timestamp","cloudcoverage","area","zipcodearea")
    DataColumns = ['productId', 'entityId', 'acquisitionDate', 'cloudCover', 'processingLevel', 'path', 'row', 'min_lat', 'min_lon', 'max_lat', 'max_lon', 'download_url']
    #Set up pulsar configuration
    nbThread = 1
    pulsarService= 'pulsar://10.0.0.5:6650'
    pulsarAdmin = 'http://10.0.0.5:8080'
    topic = 'LandSatMetaData'
    
    #Get arround a bug of the pulsar connector: try to connect first as a writer to become a reader.
    #The first connection sometimes throw an error. 
    df = sqlContext.createDataFrame([(1,1)],("id", "v"))
    ct = 0    
    while ct<1:
        try:
            df.write.format("pulsar").option("service.url", pulsarService).option("admin.url", pulsarAdmin).option("topic", "abc").save()
            break
        except:
            if ct == 1:
                raise
            ct+=1

    #Create the zipcode search tree
    zipcodeList = fetchzipcodes()
    zipsearch = zipCodeTree(list(map(zipCode,zipcodeList)))
    #Start the connection with pulsar and proccess as received

    imagelist = sqlContext.read.format("pulsar").option("service.url", pulsarService).option("admin.url", pulsarAdmin).option("topic", topic).load()
    imagelist = imagelist.rdd.map(lambda x: [decode(x)]).flatMap(processBatchEncloser)

    #Push cloudcoverage data to mysql database
    cloudcoveragedf = imagelist.map(lambda x: DFRow(*x)).toDF()
    mysqlConnector.writeToMySQL('cloudcoverage',cloudcoveragedf)
        
