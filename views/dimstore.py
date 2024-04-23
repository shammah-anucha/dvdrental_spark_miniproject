from pyspark.sql.functions import monotonically_increasing_id


def create_dimstore(table1, table2, location):
    join_expr2 = table1.staff_id == table2.manager_staff_id
    combine_staff_and_store = (
        table1.select("first_name", "last_name", "store_id", "staff_id")
        .join(table2, join_expr2, "inner")
        .drop(table1.staff_id, "manager_staff_id", "last_update", table1.store_id)
        .withColumnRenamed("first_name", "manager_first_name")
        .withColumnRenamed("last_name", "manager_last_name")
    )
    # combine_staff_and_store.show()

    join_expr3 = combine_staff_and_store.address_id == location.address_id

    include_address = combine_staff_and_store.join(location, join_expr3, "left").drop(
        combine_staff_and_store.address_id, "phone", "last_update"
    )
    # include_address.show()

    dimstore = include_address.withColumn(
        "store_key", monotonically_increasing_id()
    ).select(
        "store_key",
        "store_id",
        "address",
        "address2",
        "district",
        "city",
        "country",
        "postal_code",
        "manager_first_name",
        "manager_last_name",
    )
    return dimstore
