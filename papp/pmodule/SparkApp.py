from pyspark import SparkConf
from pyspark.sql import SparkSession

import settings

class SparkApp:

    def __init__(self, **kargs):
        self._sparkConf = SparkConf() \
                            .setAppName("papp") \
                            .setMaster("local") \
                            .setAll(sparkConfigList)
        self._spark = SparkSession \
                        .builder \
                        .enableHiveSupport() \
                        .config(conf=spark_conf) \
                        .getOrCreate()


    def getSparkSession(self) -> SparkSession:
        return self._spark


    def countFromTable(self, table) -> int:
        cnt = self._spark.sql("SELECT COUNT(*) AS cnt FROM {}".format(table))
        return cnt