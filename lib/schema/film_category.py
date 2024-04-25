from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, DateType, IntegerType


class FilmCategorySchema:
    @staticmethod
    def get_schema():
        return StructType(
            [
                StructField("film_id", IntegerType(), True),
                StructField("category_id", IntegerType(), True),
                StructField("last_update", DateType(), True),
            ]
        )
