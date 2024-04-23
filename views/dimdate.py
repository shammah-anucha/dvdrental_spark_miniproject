from pyspark.sql.functions import (
    monotonically_increasing_id,
    year,
    quarter,
    month,
    dayofweek,
    weekofyear,
    weekday,
    when,
)


def create_dimdate(table):
    create_columns = (
        table.select("return_date")
        # .withColumnRenamed("return_date", "date")
        .withColumn("date_key", monotonically_increasing_id())
        .withColumn("year", year(table["return_date"]))
        .withColumn("quarter", quarter(table["return_date"]))
        .withColumn("month", month(table["return_date"]))
        .withColumn("day", weekday(table["return_date"]))
        .withColumn("week", weekofyear(table["return_date"]))
        .withColumn(
            "is_weekend",
            when(
                (dayofweek(table["return_date"]) == 1)
                | (dayofweek(table["return_date"]) == 7),
                True,
            ).otherwise(False),
        )
    )
    dimdate = create_columns.select(
        "date_key",
        "return_date",
        "year",
        "quarter",
        "month",
        "day",
        "week",
        "is_weekend",
    ).withColumnRenamed("return_date", "date")

    return dimdate
