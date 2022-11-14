import logging
import sys

from flask import Flask
from flask import Response
from datetime import datetime

import pmodule.SparkApp as SparkApp

# -------------------------------------------------------------------------------

sys.path.append('.')

logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# Flask app
app = Flask(__name__)

# Spark app
sparkApp = SparkApp.SparkApp()


@app.route('/')
def index():
    return "Hello openshift"


# Dummy healthcheck endoint
@app.route("/health")
def getHealth():
    now = datetime.now().strftime("%H:%M:%S")
    rBody = 'UP/{}'.format(now)
    return Response(rBody)


@app.route("/sparksession")
def getSparkSession():
    session = sparkApp.getSparkSession()
    logger.info(session)
    return Response("OK")


@app.route("/tablecount")
def getTableCount():
    tables = sparkApp.getSparkSession().catalog.listTables()
    tableCount = set(map(lambda t: loader.count_from_table(t.name), tables))
    logger.info(tableCount)
    return Response("OK")


def main():
    logger.info("Starting app ...")
    app.run(host="0.0.0.0", port="8080")


if __name__ == "__main__":
    main()