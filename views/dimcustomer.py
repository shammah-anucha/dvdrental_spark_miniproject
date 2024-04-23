from pyspark.sql.functions import monotonically_increasing_id


def create_dimcustomer(location, table1, table2):
    join_expr = location.address_id == table1.address_id

    customer_table = (
        location.join(table1, join_expr, "right")
        .withColumn("customer_key", monotonically_increasing_id())
        .select(
            "customer_key",
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "address",
            "address2",
            "district",
            "city",
            "country",
            "postal_code",
            "phone",
            "active",
            "create_date",
        )
    )
    selected_rental_table = table2.select("customer_id", "rental_date", "return_date")

    join_expr2 = customer_table.customer_id == selected_rental_table.customer_id

    dimcustomer = (
        customer_table.join(selected_rental_table, join_expr2, "left")
        .drop(selected_rental_table.customer_id)
        .withColumnRenamed("rental_date", "start_date")
        .withColumnRenamed("return_date", "end_date")
    )

    return dimcustomer
