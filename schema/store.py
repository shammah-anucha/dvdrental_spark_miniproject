from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, DateType, IntegerType


class StoreSchema:
    @staticmethod
    def get_schema():
        return StructType(
            [
                StructField("store_id", IntegerType(), True),
                StructField("manager_staff_id", IntegerType(), True),
                StructField("address_id", IntegerType(), True),
                StructField("last_update", DateType(), True),
            ]
        )
