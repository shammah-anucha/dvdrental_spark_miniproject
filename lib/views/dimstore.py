def create_dimstore(staff, store, location):
    """
    location: A dataframe containing the city_id, city and country found in full_location.py
    staff: the staff dataframe staffCSVDF in main.py
    store: the store dataframe storeCSVDF in main.py

    """
    join_expr = staff.staff_id == store.manager_staff_id
    combine_staff_and_store = (
        staff.select("first_name", "last_name", "store_id", "staff_id")
        .join(store, join_expr, "inner")
        .drop(staff.staff_id, "manager_staff_id", "last_update", staff.store_id)
        .withColumnRenamed("first_name", "manager_first_name")
        .withColumnRenamed("last_name", "manager_last_name")
    )
    # combine_staff_and_store.show()

    join_expr2 = combine_staff_and_store.address_id == location.address_id

    include_address = combine_staff_and_store.join(location, join_expr2, "left").drop(
        combine_staff_and_store.address_id, "phone", "last_update"
    )
    # include_address.show()

    dimstore = include_address.select(
        include_address.store_id.alias("store_key"),
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
