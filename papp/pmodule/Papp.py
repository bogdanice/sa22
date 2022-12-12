import logging
import sys

from flask import Flask
from flask import Response
from datetime import datetime

import pmodule.SparkService as SparkService
import pmodule.schemas as schemas
import pmodule.constants as constants

# -------------------------------------------------------------------------------

sys.path.append('.')

logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Flask app
pApp = Flask(__name__)

# Spark service
sparkService = SparkService.SparkService()


@pApp.route('/')
def index():
    return "Hello openshift"


# Dummy healthcheck endoint
@pApp.route("/health")
def getHealth():
    now = datetime.now().strftime("%H:%M:%S")
    rBody = 'UP/{}'.format(now)
    return Response(rBody)


@pApp.route("/sparksession")
def getSparkSession():
    session = sparkService.getSparkSession()
    logger.info(session)
    return Response("OK")


@pApp.route("/tablecount")
def getTableCount():
    tables = sparkService.getSparkSession().catalog.listTables()
    tableCount = set(map(lambda t: loader.count_from_table(t.name), tables))
    logger.info(tableCount)
    return Response("OK")

@pApp.route("/load")
def load():
    # By default load the file under /data
    # filePath = "/workspace/sa22/papp/data/car_details_default.csv"
    filePath = "/data/car_details_default.csv"
    df = sparkService.readCsvFileToDataFrame(filePath, schemas.SCHEMA_CAR_DETAILS)
    dfCount = df.count()
    logger.info("Read file %s into dataframe. Dataframe lines=%s", filePath, dfCount)
    tableName = constants.Tables.CAR_DETAILS
    logger.info("Save as table %s", tableName)
    # sparkService.storeDataFrameAsTable(df, tableName)
    df.write.mode('overwrite').saveAsTable(tableName)
    return Response(str(dfCount))

@pApp.route("/dieselCars")
def diselCars():
    sqlQuery = "SELECT * from {} where {}='Diesel'"\
        .format(constants.Tables.CAR_DETAILS, constants.Columns.FUEL)
    sqlQuery = f'''
        SELECT * from {constants.Tables.CAR_DETAILS} 
        WHERE {constants.Columns.FUEL}="Diesel"
        '''
    logger.info("Execute query: %s", sqlQuery)
    dfDieselCars = sparkService.getSparkSession().sql(sqlQuery)
    dfDieselCarsCount = dfDieselCars.count()
    return Response(str(dfDieselCarsCount))