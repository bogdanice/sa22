from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame

import pmodule.settings as settings

class SparkService:

    def __init__(self, **kargs):
        self._sparkConf = SparkConf() \
                            .setAppName("papp") \
                            .setAll(settings.sparkConfigList)
        self._spark = SparkSession \
                        .builder \
                        .enableHiveSupport() \
                        .master("local") \
                        .config(conf=self._sparkConf) \
                        .getOrCreate()


    def getSparkSession(self) -> SparkSession:
        return self._spark


    def countFromTable(self, table) -> int:
        return self._spark.sql("SELECT COUNT(*) AS cnt FROM {}".format(table))

    
    def readCsvFileToDataFrame(self, filePath, schema, delimiter=",", header=True) -> DataFrame:
        return self._spark.read.format("csv") \
                        .option("header", True) \
                        .option("sep", delimiter) \
                        .schema(schema) \
                        .load(filePath)

    def storeDataFrameAsTable(self, df, tableName):
        # df.write.saveAsTable(name=tableName, format="parquet", mode="overwrite", compress=False)
        df.write.mode('overwrite').saveAsTable(tableName)