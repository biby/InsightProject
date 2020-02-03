def importModules(x):
    from zipcode import zipCode
    from satelliteImage import satelliteImage
    import PIL
    from PIL import Image    
    import numpy as np
    import boto3
    return x

'''
dates = ("2018-03-19 00:00:00",  "2018-03-20 00:00:00")
q2 = """CAST(acquisitionDate AS INT)
        BETWEEN unix_timestamp('{0}', 'yyyy-MM-dd HH:mm:ss')
        AND unix_timestamp('{1}', 'yyyy-MM-dd HH:mm:ss')""".format(*dates)

df2 = df.where(q2)
df2.count()
df2.show()
'''

def getzipcodes(minmaxlatlong,zipcodelist):
    '''
        Returns the list of zipcodes that intersects with the latitude longitude box provided.
    '''
    intersectingZipcodes = zipcodelist.filter(lambda zipcode: zipcode.intersects(minmaxlatlong))
    return intersectingZipcodes

def getImage(s3Bucket,url):
    #os.system("pip3 install numpy")
    """import boto3
    import PIL
    import PIL.Image"""
    s3 = boto3.client('s3')
    tmp = tempfile.NamedTemporaryFile()
    with open(tmp.name,'wb') as f:
        s3.download_fileobj(s3Bucket, url, f)
    img = PIL.Image.open(tmp.name)   
    img_arr = np.array(img)
    return img_arr

def downloadImage(url):
    start_time = time.time()
    image = getImage(s3Bucket,url)
    print('download time: ', time.time()-start_time)
    return image

def extractImage(line):
    import time
    start_full = time.time()
    l = line.split(',')
    url = l[-1]
    path = url.split('/')
    s3Bucket = path[3]
    path = path[4:] #Extract path in the s3 bucket
    fileName = path[-2]+'_B9.TIF'
    path[-1]=fileName
    url = '/'.join(path)
    
    start_time = time.time()        
    minmaxlatlong = list(map(float,l[7:11]))
    zips = intersectingZipCode(minmaxlatlong)
    print(len(zips), minmaxlatlong)    
    print('Intersecting Time: ', time.time()-start_time)
    #If no zipcode intersects, does not download image
    if len(zips)==0:
        return []
    else:
        return [len(zips)]
    image = downloadImage(url)

    #For each zip code intersecting the image, computes the cloud coverage.
    start_time = time.time()        
    li = list(map(lambda zp: crop(image,minmaxlatlong,zp),zips))
    print('Cropping Time: ', time.time()-start_time)
    print('Full image process: ', time.time()-start_full)
    return li

def intersectingZipCode(minmaxlatlong):    
    #return filter(lambda x: zipCode(x).intersects(minmaxlatlong), zipcodeList)
    return list(map(lambda x: x.raw, zipsearch.intersects(minmaxlatlong)))
        

def intersectionFilter(x):
    line = x[0][1]
    zipcode = zipCode(x[1])
    l = line.split(',')
    #Get the zipcodes that intersect the image
    minmaxlatlong = list(map(float,l[7:11]))
    return zipcode.intersects(minmaxlatlong)

def uploadzipcodes():
    zipcodes = sc.textFile('s3a://ideanalytics/zcta2010.csv').map(importModules)
    zc =zipcodes.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)
    return zc.collect()#.map(zipCode)

def uploadsolarpanels():
    from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
    import pyspark.sql.functions
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

def crop(image,minmaxlatlong,zp):   
    img = satelliteImage(image,minmaxlatlong)
    zipc = zipCode(zp)    
    croppedImg = img.crop([zipc.minLatitude, zipc.minLongitude, zipc.maxLatitude, zipc.maxLongitude])
    cover = croppedImg.cloudCover()
    return (cover,minmaxlatlong,zp.split(',')[0])

'''with open(tmp.name,'wb') as f:
    
    s3.download_fileobj(bucketName, 'c1/L8/139/045/LC08_L1TP_139045_20170304_20170316_01_T1/LC08_L1TP_139045_20170304_20170316_01_T1_B9.TIF', f)

img = Image.open(tmp.name)
img_arr = np.array(img)
'''
'''


def get_all(line):
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(extractImage, line) for url in urls.toLocalIterator()]
    # the end of the "with" block will automatically wait
    # for all of the executor's tasks to complete

    for fut in futures:
        if fut.exception() is not None:
            print('{}: {}'.format(fut.exception(), 'ERR')
        else:
            print('{}: {}'.format(fut.result(), 'OK')
'''

if __name__=='__main__':
    from pyspark import SparkContext, SparkConf,SQLContext
    import pyspark
    conf = SparkConf().setAppName('test')
    sc = SparkContext(conf = conf, pyFiles=['zipcode.py', 'solarirradiance.py','satelliteImage.py','SparktomySQL.py'])
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
    print(zipcodeList[:100])
    start_time = time.time()
    zipsearch = zipCodeTree(list(map(zipCode,zipcodeList)))
    print('zipCodeTree init :', time.time() - start_time)
    
    
    #imagelist = sc.textFile('s3a://ideanalytics/scene_list_LandSat_Data.csv').mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)
    imagelist = sqlContext.read.csv('s3a://ideanalytics/scene_list_LandSat_Data.csv',header='true')
    
    #Remove image already processed
    #Download the csv file containing satellite images metadata
    proccessedImages = mysqlConnector.loadImageIDInDatabase()
    imagelist = imagelist.join(proccessedImages, imagelist["entityId"] == proccessedImages["id"],"leftanti")
    
    print(imagelist.show())
    #imagelist = imagelist.rdd.map(lambda x: ','.join(x))
    
    #ProcessImages
    #im2 = imagelist.map(importModules).flatMap(extractImage)
    #print('\n\n\n\n',im2.reduce(lambda x,y:x+y),'\n\n\n\n')
    #print('\n\n\n\n',im2.count(),'\n\n\n\n')
    #im2 = im2.filter(lambda x: x[0]>0.0)
    #print(im2.take(10000))
    
