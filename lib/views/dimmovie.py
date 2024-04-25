def create_dimmovies(
    film,
    language,
):
    """
    film: the film dataframe filmCSVDF in main.py
    language: the language dataframe called languageCSVDF in main.py

    """
    join_expr = film.language_id == language.language_id
    dimmovies = (
        film.join(language, join_expr, "inner")
        .drop(
            "replacement_cost",
            "fulltext",
            "last_update",
            "rental_rate",
            "original_language_id",
            film.language_id,
            language.language_id,
        )
        .withColumnRenamed("name", "language")
    ).select(
        film.film_id.alias("movie_key"),
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
