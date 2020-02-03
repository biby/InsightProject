from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
import pyspark.sql.functions
from SparktomySQL import MySQLConnector

class SolarPanels:

    def __init__(self,sqlContext,solarpanelcsvurl='s3a://ideanalytics/solar_panel_install.csv',zipcodescsvurl = 's3a://ideanalytics/zcta2010.csv'):
        '''
            Merge solar Panel dataset and zipcode dataset to an dataframe stored in self.zipcodes
        '''
        self.sqlContext = sqlContext
        self.solarpanelurl = solarpanelcsvurl
        self.zipcodeurl = zipcodescsvurl
        self.uploadsolarpaneldf()
        self.solarPaneldfcleanup()
        self.uploadZipcode()
        self.mergeZipCodeAndPV()
        del self.solarPanelDf
    
    def updateDataBase(self,mysqlConnector,table='solarPanels',mode='overwrite'):
        mysqlConnector.writeToMySQL(table,self.zipcodes,mode=mode)

    def uploadsolarpaneldf(self):

        self.solarPanelDf = self.sqlContext.read.csv(self.solarpanelurl,header='true',inferSchema='true')
        
    def solarPaneldfcleanup(self):

        #Exctract usefullcollumns
        efficiency = 'Module Efficiency #1'
        self.solarPanelDf = self.solarPanelDf.select('System Size', efficiency, 'Zip Code')

        #Replace unknown efficiency to defaultvalue
        efficiencyDefault = .14
        self.solarPanelDf = self.solarPanelDf.withColumn(efficiency, pyspark.sql.functions.when(pyspark.sql.functions.col(efficiency)==-9999.0, pyspark.sql.functions.lit(efficiencyDefault)).otherwise(self.solarPanelDf[efficiency]))
        
        #Clean ZipCode to 5 digit zicode
        self.solarPanelDf = self.solarPanelDf.withColumn('Zip Code', pyspark.sql.functions.regexp_replace('Zip Code', '(.+?)-.+','$1'))
        
        #Aggregate solar panels data by zipcode, adding the size and averaging the efficiency 
        self.solarPanelDf = self.solarPanelDf.withColumn('Zip Code', self.solarPanelDf['Zip Code'].cast(IntegerType()))
        self.solarPanelDf = self.solarPanelDf.groupBy('Zip Code').agg(pyspark.sql.functions.sum('System Size'),pyspark.sql.functions.avg(efficiency))
 
    def uploadZipcode(self):
        #Download and select columns
        schema = StructType([StructField("ZCTA", IntegerType()),StructField("LandSqMt", IntegerType()),StructField("Latitude", FloatType()),StructField("Longitude", FloatType())])
        self.zipcodes = self.sqlContext.read.csv(self.zipcodeurl,header='true',schema = schema)
        self.zipcodes = self.zipcodes.select('ZCTA', 'LandSqMt', 'Latitude','Longitude')

    def mergeZipCodeAndPV(self):
        self.zipcodes = self.zipcodes.join(self.solarPanelDf,self.solarPanelDf['Zip Code'] == self.zipcodes['ZCTA'], 'inner').drop('Zip Code').withColumnRenamed('ZCTA', 'ZipCode')
