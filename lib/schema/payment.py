from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, DoubleType


class PaymentSchema:
    @staticmethod
    def get_schema():
        return StructType(
            [
                StructField("payment_id", IntegerType(), True),
                StructField("customer_id", IntegerType(), True),
                StructField("staff_id", IntegerType(), True),
                StructField("rental_id", IntegerType(), True),
                StructField("amount", DoubleType(), True),
                StructField("payment_date", DateType(), True),
                StructField("last_update", DateType(), True),
            ]
        )
