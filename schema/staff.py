from pyspark.sql import *
from pyspark.sql.types import (
    StructType,
    StructField,
    DateType,
    IntegerType,
    StringType,
    BooleanType,
)


class StaffSchema:
    @staticmethod
    def get_schema():
        return StructType(
            [
                StructField("staff_id", IntegerType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("address_id", IntegerType(), True),
                StructField("picture", StringType(), True),
                StructField("email", StringType(), True),
                StructField("store_id", IntegerType(), True),
                StructField("active", BooleanType(), True),
                StructField("username", StringType(), True),
                StructField("password", StringType(), True),
                StructField("last_update", DateType(), True),
            ]
        )
