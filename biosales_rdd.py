from biosales_dataframe import get_df_from_csv

def get_rdd_from_csv(spark, response_csv):
    return get_df_from_csv(spark, response_csv).rdd