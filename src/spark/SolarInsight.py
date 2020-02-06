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



def fetchzipcodes():
    zipcodes = sc.textFile('s3a://ideanalytics/zcta2010.csv')
    zc =zipcodes.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)
    return zc.collect()


def processBatchEncloser(data):
    imageProcessor = ImageProcessor(zipsearch,nbThread)
    return imageProcessor.processBatch(data)

if __name__=='__main__':

    #Configuring spark
    conf = SparkConf().setAppName('test')
    sc = SparkContext(conf = conf, pyFiles=['zipcode.py', 'solarirradiance.py','satelliteImage.py','SparktomySQL.py','imageProcessor.py','solarPanels.py'])
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
    
    # Agreage solar panels by zipcode location return a dataFrame and Uudate the mysqldatabase
    #solarUpdate = SolarPanels(sqlContext,'s3a://ideanalytics/solar_panel_install.csv','s3a://ideanalytics/zcta2010.csv')
    #solarUpdate.updateDataBase(mysqlConnector,table='solarPanels', mode='overwrite')

    #Create a zipCode search tree
    zipcodeList = fetchzipcodes()
    start_time = time.time()
    zipsearch = zipCodeTree(list(map(zipCode,zipcodeList)))
    print('zipCodeTree init :', time.time() - start_time)
    
    #imagelist = sc.textFile('s3a://ideanalytics/scene_list_LandSat_Data.csv').mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)
    imagelist = sqlContext.read.csv('s3a://ideanalytics/scene_list_LandSat_Data.csv',header='true')
    
    #Remove duplicates
    columns = imagelist.columns
    aggregationdic = {column : 'max' for column in columns}
    imagelist = imagelist.groupBy(imagelist["entityId"]).agg(aggregationdic).select([pyspark.sql.functions.col('max('+column+')').alias(column) for column in columns])
    
    #Remove image already processed
    #Download the csv file containing satellite images metadata
    proccessedImages = mysqlConnector.loadImageIDInDatabase()
    imagelist = imagelist.join(proccessedImages, imagelist["entityId"] == proccessedImages["id"],"leftanti")
    
    #Remove images not intersecting any zipcode
    
    imagelist = imagelist.rdd
    imagelist = imagelist.filter(lambda x: len(zipsearch.intersects(list(map(float,x[7:11]))))>0)

    
     
    #Group rdd elements by small batches to speed up download time
    nbThread = 10

    imlen = imagelist.count()
    nbThread = min(nbThread,imlen) 
    imagelist = imagelist.map(lambda x: ','.join(x))
    imagelist = imagelist.zipWithIndex().map(lambda x: (x[1]%(imlen//nbThread),x[0]))
    #print(imagelist.take(10))
    
    imagelist = imagelist.groupByKey().mapValues(list).map(lambda x: x[1])
    print(imagelist.take(10))

    #ProcessImages
    imagelist = imagelist.flatMap(processBatchEncloser)
    
    #Push cloudcoverage data to mysql database
    DFRow = Row("id","zipcode","timestamp","cloudcoverage","area","zipcodearea")
    cloudcoveragedf = imagelist.map(lambda x: DFRow(*x)).toDF()
    mysqlConnector.writeToMySQL('cloudcoverage',cloudcoveragedf)
    
    
