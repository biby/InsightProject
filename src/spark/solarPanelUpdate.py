import SolarInsight


if __name__="__main__':
    #Configuring spark
    conf = SparkConf().setAppName('test')
    sc = SparkContext(conf = conf, pyFiles=['zipcode.py', 'solarirradiance.py','satelliteImage.py','SparktomySQL.py','imageProcessor.py','solarPanels.py'])
    sqlContext = SQLContext(sc)

    #Set up S3 access keys for spark
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.environ["AWS_ACCESS_KEY"])
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ["AWS_SECRET_KEY"])
    sqlhost='10.0.0.8'
    sqluser='ubuntu'
    sqlpassword = os.environ["MYSQL_SECRET_KEY"]
    sqldatabase='zipcloud'
    mysqlConnector = MySQLConnector(sqlContext,host=sqlhost, user=sqluser, password=sqlpassword,databaseName=sqldatabase)
    
    #Agregate solar panels by zipcode location return a dataFrame and Update the mysqldatabase
    solarUpdate = SolarPanels(sqlContext,'s3a://ideanalytics/solar_panel_install.csv','s3a://ideanalytics/zcta2010.csv')
    solarUpdate.updateDataBase(mysqlConnector,table='solarPanels', mode='overwrite')
