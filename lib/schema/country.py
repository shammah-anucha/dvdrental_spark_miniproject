from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType


class CountrySchema:
    @staticmethod
    def get_schema():
        return StructType(
            [
                StructField("country_id", IntegerType(), True),
                StructField("country", StringType(), True),
                StructField("last_update", DateType(), True),
            ]
        )
