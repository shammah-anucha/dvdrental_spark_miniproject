from pyspark.sql.functions import monotonically_increasing_id, date_format


def create_factsales(rental, inventory, payment):
    """
    rental: the rental dataframe rentalCSVDF in main.py
    inventory: the inventory dataframe inventoryCSVDF in main.py
    payment: the payment dataframe paymentCSVDF in main.py

    """
    join_payment_and_rental_expr = rental.rental_id == payment.rental_id
    payment_and_rental_table = rental.join(
        payment, join_payment_and_rental_expr, "inner"
    ).select(
        "payment_id",
        rental.rental_id,
        "amount",
        "inventory_id",
        "payment_date",
        payment.customer_id,
    )

    join_tables_to_inventory_expr = (
        inventory.inventory_id == payment_and_rental_table.inventory_id
    )
    factsales = (
        inventory.join(payment_and_rental_table, join_tables_to_inventory_expr, "inner")
        .withColumn("sales_key", monotonically_increasing_id())
        .select(
            "sales_key",
            date_format(payment_and_rental_table.payment_date, "yyyyMMdd")
            .cast("int")
            .alias("date_key"),
            payment_and_rental_table.customer_id.alias("customer_key"),
            inventory.film_id.alias("movie_key"),
            inventory.store_id.alias("store_key"),
            payment_and_rental_table.amount.alias("sales_amount"),
        )
    )
    return factsales
