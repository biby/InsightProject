class MySQLConnector:

    def __init__(self,sqlContext,host,user,password,databaseName,driver='com.mysql.jdbc.Driver'):
        self.sqlContext =sqlContext
        self.host = host
        self.user = user
        self.password = password
        self.database = databaseName
        self.driver = driver
                
    @property
    def url(self):
        return 'jdbc:mysql://'+self.host+'/'+self.database

    def writeToMySQL(self,table,df,mode='append'):
        df.write.format('jdbc').options(
          url=self.url,
          driver=self.driver,
          dbtable=table,
          user=self.user,
          password=self.password).mode(mode).save()

    def loadImageIDInDatabase(self):
        table = 'cloudcoverage'
        querry = '(SELECT DISTINCT id FROM '+ table +') AS id'
        imageID = self.sqlContext.read.format("jdbc").options(driver=self.driver,
            url=self.url,
            user=self.user,
            password=self.password).option('dbtable',querry).load()
        return imageID


#cloudcoverage columns: [id,zipcode,timestamp,cloudcoverage,area]
