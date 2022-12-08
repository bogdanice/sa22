#------------------------------------------------------------------------------
# Schema definitions for Spark dataframes
#------------------------------------------------------------------------------

from pyspark.sql.types import *
from pmodule.constants import Columns

# name,year,selling_price,km_driven,fuel,seller_type,transmission,owner
SCHEMA_CAR_DETAILS = StructType() \
    .add(Columns.NAME, StringType(), True) \
    .add(Columns.YEAR, IntegerType(), True) \
    .add(Columns.SELLING_PRICE, IntegerType(), True) \
    .add(Columns.KM_DRIVEN, IntegerType(), True) \
    .add(Columns.FUEL, StringType(), True) \
    .add(Columns.SELLER_TYPE, StringType(), True) \
    .add(Columns.TRANSMISSION, StringType(), True) \
    .add(Columns.OWNER, StringType(), True)
