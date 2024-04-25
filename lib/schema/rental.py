from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, DateType, IntegerType


class RentalSchema:
    @staticmethod
    def get_schema():
        return StructType(
            [
                StructField("rental_id", IntegerType(), True),
                StructField("rental_date", DateType(), True),
                StructField("inventory_id", IntegerType(), True),
                StructField("customer_id", IntegerType(), True),
                StructField("return_date", DateType(), True),
                StructField("staff_id", IntegerType(), True),
                StructField("last_update", DateType(), True),
            ]
        )
