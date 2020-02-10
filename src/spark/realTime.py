import SolarInsight

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
    DFRow = Row("id","zipcode","timestamp","cloudcoverage","area","zipcodearea")
    
    #Set up pulsar configuration
    nbThread = 1
    pulsarService= 'pulsar://10.0.0.5:6650'
    pulsarAdmin = 'http://10.0.0.5:8080'
    topic = 'LandSatMetaData'
        
    #Start the connection with pulsar and proccess as received
    while(True):
        imagelist = spark.read.format("pulsar").option("service.url", pulsarService).option("admin.url", pulsarAdmin).option("topic", topic).load()
        imagelist = imagelist.map(lambda x : [x]).flatMap(processBatchEncloser)
    
        #Push cloudcoverage data to mysql database
        cloudcoveragedf = imagelist.map(lambda x: DFRow(*x)).toDF()
        mysqlConnector.writeToMySQL('cloudcoverage',cloudcoveragedf)

