'''
Created on Nov 17, 2020

@author: Vignesh.Asokan
'''
from pyspark.sql.types import *
from pyspark import HiveContext,SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

class Reader(object):
    def __init__(self, filePath,sparkSession):
        '''
        Constructor
        '''
        self.filePath=filePath
        self.sparkSession=sparkSession

    def read_csv(self,fileName):
        print (self.filePath)
        wandRawDf=self.sparkSession.read.csv(self.filePath+"/"+fileName,, header='true')
        return wandRawDf
     
    def read_df_data(self,sql):
        df = self.sparkSession.sql(sql)
        df.show()
        return df
    
    def prep_data(self, sqltext, tablename):
        df = self.sparkSession.sql(sqltext)
        df.registerTempTable(tablename)
        print("Temporary spark table registered as %s" % tablename)
        return df

