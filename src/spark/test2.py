"""def importModules(x):
    from zipcode import zipCode
    from satelliteImage import satelliteImage
    import PIL
    from PIL import Image    
    import numpy as np
    import boto3
    return x
"""


def uploadzipcodes():
    zipcodes = sc.textFile('s3a://ideanalytics/zcta2010.csv')#.map(importModules)
    zc =zipcodes.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)
    return zc.collect()#.map(zipCode)

def uploadsolarpanels():
    from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
    

    schema = StructType([StructField("ZCTA", IntegerType()),StructField("LandSqMt", IntegerType()),StructField("Latitude", FloatType()),StructField("Longitude", FloatType())])
    efficiency = 'Module Efficiency #1'
    solarPanels = sqlContext.read.csv('s3a://ideanalytics/solar_panel_install.csv',header='true',inferSchema='true')
    solarPanels = solarPanels.select('System Size', efficiency, 'Zip Code')
    solarPanels = solarPanels.withColumn(efficiency, pyspark.sql.functions.when(pyspark.sql.functions.col(efficiency)==-9999.0,pyspark.sql.functions.lit(.14)).otherwise(solarPanels[efficiency]))
    solarPanels = solarPanels.withColumn('Zip Code', pyspark.sql.functions.regexp_replace('Zip Code', '(.+?)-.+','$1'))
    solarPanels = solarPanels.withColumn('Zip Code', solarPanels['Zip Code'].cast(IntegerType()))
    solarPanels = solarPanels.groupBy('Zip Code').agg(pyspark.sql.functions.sum('System Size'),pyspark.sql.functions.avg(efficiency))
    zipcodeRdd = sqlContext.read.csv('s3a://ideanalytics/zcta2010.csv',header='true',schema = schema)
    zipcodeRdd = zipcodeRdd.select('ZCTA', 'LandSqMt', 'Latitude','Longitude')
    zipcodeRdd = zipcodeRdd.join(solarPanels,solarPanels['Zip Code'] == zipcodeRdd['ZCTA'], 'inner')
    zipcodeRdd = zipcodeRdd.drop('Zip Code')
    zipcodeTdd = zipcodeRdd.withColumnRenamed('ZCTA', 'ZipCode')
    return zipcodeRdd


def processBatchEncloser(data):
    imageProcessor = ImageProcessor(zipsearch,nbThread)
    return imageProcessor.processBatch(data)

if __name__=='__main__':
    from pyspark import SparkContext, SparkConf,SQLContext
    import pyspark
    conf = SparkConf().setAppName('test')
    sc = SparkContext(conf = conf, pyFiles=['zipcode.py', 'solarirradiance.py','satelliteImage.py','SparktomySQL.py','imageProcessor.py'])
    sqlContext = SQLContext(sc)

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
    
    #Create a mysqlclient
    mysqlConnector = MySQLConnector(sqlContext,host='10.0.0.8', user='ubuntu', password='Azertyu1!',databaseName='zipcloud')
    #Set up S3 access keys for spark
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.environ["AWS_ACCESS_KEY"])
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ["AWS_SECRET_KEY"])
    
    # Agreage solar panels by zipcode location return a dataFrame
    #solarPanels = uploadsolarpanels()
    #mysqlConnector.writeToMySQL('solarPanels',solarPanels,mode='overwrite')
    
    #Create a zipCode search tree
    zipcodeList = uploadzipcodes()
    #print(zipcodeList[:100])
    start_time = time.time()
    zipsearch = zipCodeTree(list(map(zipCode,zipcodeList)))
    print('zipCodeTree init :', time.time() - start_time)
    
    nbThread = 20    
    
    
    
    #imagelist = sc.textFile('s3a://ideanalytics/scene_list_LandSat_Data.csv').mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)
    imagelist = sqlContext.read.csv('s3a://ideanalytics/scene_list_LandSat_Data.csv',header='true')
    
    #Remove image already processed
    #Download the csv file containing satellite images metadata
    proccessedImages = mysqlConnector.loadImageIDInDatabase()
    imagelist = imagelist.join(proccessedImages, imagelist["entityId"] == proccessedImages["id"],"leftanti")
    
    imlen = imagelist.count()
    import random
    imagelist = imagelist.rdd.map(lambda x: ','.join(x))
    imagelist = imagelist.map(lambda x: (random.randint(0,(imlen//nbThread)),x)).groupByKey().mapValues(list).map(lambda x: x[1])
    #print(imagelist.take(10))
    #ProcessImages
    imagelist = imagelist.flatMap(processBatchEncloser)
    print(imagelist.toDF().show())
    #print('\n\n\n\n',im2.reduce(lambda x,y:x+y),'\n\n\n\n')
    #print('\n\n\n\n',im2.count(),'\n\n\n\n')
    #im2 = im2.filter(lambda x: x[0]>0.0)
    #print(imagelist.take(100000)[:100])
    
