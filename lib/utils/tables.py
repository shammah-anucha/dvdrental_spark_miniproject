def load_df_fail_fast(spark, schema, data_file):
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(schema)
        .option("mode", "FAILFAST")
        .csv(data_file)
    )


def load_df_permissive(spark, schema, data_file):
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(schema)
        .option("mode", "PERMISSIVE")
        .csv(data_file)
    )
