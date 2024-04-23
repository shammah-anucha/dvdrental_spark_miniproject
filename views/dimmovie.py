from pyspark.sql.functions import monotonically_increasing_id


def create_dimmovies(
    table1,
    table2,
):
    join_expr = table1.language_id == table2.language_id
    dimmovies = (
        table1.join(table2, join_expr, "inner")
        .drop(
            "replacement_cost",
            "fulltext",
            "last_update",
            "rental_rate",
            "original_language_id",
            table2.language_id,
            table2.language_id,
        )
        .withColumnRenamed("name", "language")
        .withColumn("movie_key", monotonically_increasing_id())
    ).select(
        "movie_key",
        "film_id",
        "title",
        "description",
        "release_year",
        "language",
        "rental_duration",
        "length",
        "rating",
        "special_features",
    )

    return dimmovies
