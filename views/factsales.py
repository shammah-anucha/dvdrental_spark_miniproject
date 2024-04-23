from pyspark.sql.functions import monotonically_increasing_id


def create_factsales(date, customer, movie, store, payment):
    date_table = date.select("date_key")
    customer_table = customer.select("customer_key")
    movie_table = movie.select("movie_key")
    store_table = store.select("store_key")
    payment_table = payment.select("amount").withColumnRenamed("amount", "sales_amount")

    return date_table.union(customer_table)
