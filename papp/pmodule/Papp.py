import logging
import sys

from flask import Flask
from flask import Response
from datetime import datetime

import pmodule.SparkService as SparkService

# -------------------------------------------------------------------------------

sys.path.append('.')

logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Flask app
pApp = Flask(__name__)

# Spark pApp
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
