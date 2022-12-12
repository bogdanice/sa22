import logging
import sys

from flask import Flask
from flask import Response
from datetime import datetime

import pmodule.SparkService as SparkService
import pmodule.Papp as Papp

# -------------------------------------------------------------------------------

sys.path.append('.')

logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main():
    logger.info("Starting app ...")
    Papp.pApp.run(host="0.0.0.0", port="8080")


if __name__ == "__main__":
    main()