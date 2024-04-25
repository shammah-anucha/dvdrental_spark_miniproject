from pyspark.sql.functions import (
    year,
    quarter,
    month,
    dayofweek,
    weekofyear,
    weekday,
    when,
    date_format,
)


def create_dimdate(payment):
    """
    payment: the payment dataframe paymentCSVDF in main.py

    """
    create_columns = (
        payment.select("payment_date")
        # .withColumnRenamed("payment_date", "date")
        .withColumn(
            "date_key",
            date_format(payment.payment_date, "yyyyMMdd").cast("int").alias("date_key"),
        )
        .withColumn("year", year(payment["payment_date"]))
        .withColumn("quarter", quarter(payment["payment_date"]))
        .withColumn("month", month(payment["payment_date"]))
        .withColumn("day", weekday(payment["payment_date"]))
        .withColumn("week", weekofyear(payment["payment_date"]))
        .withColumn(
            "is_weekend",
            when(
                (dayofweek(payment["payment_date"]) == 1)
                | (dayofweek(payment["payment_date"]) == 7),
                True,
            ).otherwise(False),
        )
    )
    dimdate = create_columns.select(
        "date_key",
        "payment_date",
        "year",
        "quarter",
        "month",
        "day",
        "week",
        "is_weekend",
    ).withColumnRenamed("payment_date", "date")

    return dimdate
