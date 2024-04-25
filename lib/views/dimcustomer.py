from pyspark.sql.functions import monotonically_increasing_id, current_timestamp


def create_dimcustomer(location, customer):
    """
    location: A dataframe containing the city_id, city and country found in full_location.py
    customer: the customer dataframe customerCSVDF in main.py

    """
    join_expr = location.address_id == customer.address_id

    dimcustomer = location.join(customer, join_expr, "right").select(
        customer.customer_id.alias("customer_key"),
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
        current_timestamp().alias("start_date"),
        current_timestamp().alias("end_date"),
    )
    # selected_rental_table = table2.select("customer_id", "rental_date", "return_date")

    # join_expr2 = customer_table.customer_id == selected_rental_table.customer_id

    # dimcustomer = (
    #     customer_table.join(selected_rental_table, join_expr2, "left")
    #     .drop(selected_rental_table.customer_id)
    #     .withColumnRenamed("rental_date", "start_date")
    #     .withColumnRenamed("return_date", "end_date")
    # )

    return dimcustomer
