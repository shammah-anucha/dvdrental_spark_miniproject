from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType


class CitySchema:
    @staticmethod
    def get_schema():
        return StructType(
            [
                StructField("city_id", IntegerType(), True),
                StructField("city", StringType(), True),
                StructField("country_id", IntegerType(), True),
                StructField("last_update", DateType(), True),
            ]
        )
