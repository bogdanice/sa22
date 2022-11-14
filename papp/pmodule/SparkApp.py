from pyspark import SparkConf
from pyspark.sql import SparkSession

import pmodule.settings as settings

class SparkApp:

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
        cnt = self._spark.sql("SELECT COUNT(*) AS cnt FROM {}".format(table))
        return cnt