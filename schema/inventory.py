from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, DateType, IntegerType


class InventorySchema:
    @staticmethod
    def get_schema():
        return StructType(
            [
                StructField("inventory_id", IntegerType(), True),
                StructField("film_id", IntegerType(), True),
                StructField("store_id", IntegerType(), True),
                StructField("last_update", DateType(), True),
            ]
        )
