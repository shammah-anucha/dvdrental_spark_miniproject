from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType


class CategorySchema:
    @staticmethod
    def get_schema():
        return StructType(
            [
                StructField("category_id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("last_update", DateType(), True),
            ]
        )
