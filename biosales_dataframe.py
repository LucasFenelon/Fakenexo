
def get_df_from_csv(spark, response_csv):
    sc = spark.sparkContext
    df = spark.read \
        .option("header", "true") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .option("delimiter", ",") \
        .csv(sc.parallelize(response_csv.decode("utf-8").splitlines()))

    return df