from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType


class ActorSchema:
    @staticmethod
    def get_schema():
        return StructType(
            [
                StructField("actor_id", IntegerType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("last_update", DateType(), True),
            ]
        )
