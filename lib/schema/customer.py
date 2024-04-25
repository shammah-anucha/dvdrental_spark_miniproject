from pyspark.sql import *
from pyspark.sql.types import (
    StructType,
    StructField,
    DateType,
    IntegerType,
    StringType,
    BooleanType,
)


class CustomerSchema:
    @staticmethod
    def get_schema():
        return StructType(
            [
                StructField("customer_id", IntegerType(), True),
                StructField("store_id", IntegerType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("address_id", IntegerType(), True),
                StructField("active", BooleanType(), True),
                StructField("create_date", DateType(), True),
                StructField("last_update", DateType(), True),
            ]
        )
