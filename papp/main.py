import logging

from flask import Flask
from flask import Response
from datetime import datetime

import pmodule.SparkApp as SparkApp

# -------------------------------------------------------------------------------

logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# Flask app
app = Flask(__name__)


@app.route('/')
def index():
    return "Hello openshift"


# Dummy healthcheck endoint
@app.route("/health")
def getHealth():
    now = datetime.now().strftime("%H:%M:%S")
    rBody = 'UP/{}'.format(now)
    return Response(rBody)


@app.route("/tablecount")
def getTableCount():
    tables = SparkApp.getSparkSession().catalog.listTables()
    rBody = set(map(lambda t: loader.count_from_table(t.name), tables))
    return Response(rBody)


def main():
    logger.info("Starting app ...")
    app.run(host="0.0.0.0", port="8080")


if __name__ == "__main__":
    main()