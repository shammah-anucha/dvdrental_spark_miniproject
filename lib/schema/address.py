from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType


class AddressSchema:
    @staticmethod
    def get_schema():
        return StructType(
            [
                StructField("address_id", IntegerType(), True),
                StructField("address", StringType(), True),
                StructField("address2", StringType(), True),
                StructField("district", IntegerType(), True),
                StructField("city_id", IntegerType(), True),
                StructField("postal_code", IntegerType(), True),
                StructField("phone", IntegerType(), True),
                StructField("last_update", DateType(), True),
            ]
        )
