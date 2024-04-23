from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, DateType, IntegerType


class FilmActorSchema:
    @staticmethod
    def get_schema():
        return StructType(
            [
                StructField("actor_id", IntegerType(), True),
                StructField("film_id", IntegerType(), True),
                StructField("last_update", DateType(), True),
            ]
        )
