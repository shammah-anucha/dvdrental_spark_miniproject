from pyspark.sql import *
from pyspark.sql.types import (
    StructType,
    StructField,
    DateType,
    IntegerType,
    StringType,
    DoubleType,
)


class FilmSchema:
    @staticmethod
    def get_schema():
        return StructType(
            [
                StructField("film_id", IntegerType(), True),
                StructField("title", StringType(), True),
                StructField("description", StringType(), True),
                StructField("release_year", IntegerType(), True),
                StructField("language_id", IntegerType(), True),
                StructField("original_language_id", IntegerType(), True),
                StructField("rental_duration", IntegerType(), True),
                StructField("rental_rate", DoubleType(), True),
                StructField("length", IntegerType(), True),
                StructField("replacement_cost", DoubleType(), True),
                StructField("rating", StringType(), True),
                StructField("special_features", StringType(), True),
                StructField("last_update", DateType(), True),
            ]
        )
