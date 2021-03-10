from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from biosales_web import get_pull_url_responses
from biosales_service import get_rdd_qt_avg_by_region_item, get_df_qt_avg_by_region_item

# make a pull of urls to get content
pull_urls = ["https://s3-us-west-2.amazonaws.com/bioanalytics-interview/dataset/biosales.csv",
             "https://s3-us-west-2.amazonaws.com/bioanalytics-interview/dataset/biosales-buyers.csv",
             "https://s3-us-west-2.amazonaws.com/bioanalytics-interview/dataset/biosales-sellers.csv",
             "https://s3-us-west-2.amazonaws.com/bioanalytics-interview/dataset/biosales-regions.csv",
             "https://s3-us-west-2.amazonaws.com/bioanalytics-interview/dataset/biosales-items.csv",
             "https://s3-us-west-2.amazonaws.com/bioanalytics-interview/dataset/biosales-platforms.csv"]
path_rdd = "results/rdd/{}"
path_dataframe = "results/dataframe/{}"

# do not modify this method/function
def price_by_region_rdd():
    rdd = get_rdd_qt_avg_by_region_item(
        SparkSession.builder.master("local").appName("biosales").getOrCreate(),
        get_pull_url_responses(urls=pull_urls))

    rdd.saveAsTextFile(path_rdd.format("tuple"))
    rdd.toDF().select(col("_1").alias("nome_item"), col("_2").alias("quantidade_item"), col("_3").alias("nome_regiao"))\
        .write.partitionBy("nome_regiao").format("parquet").save(path_rdd.format("parquet"))
    rdd.toDF().select(col("_1").alias("nome_item"), col("_2").alias("quantidade_item"), col("_3").alias("nome_regiao")) \
        .coalesce(1).write.partitionBy("nome_regiao").format("csv").save(path_rdd.format("csv"))
    rdd.toDF().coalesce(1).write.csv("biosales-by-region-rdd")

    return rdd

# do not modify this method/function
def price_by_region_sql():
    df = get_df_qt_avg_by_region_item(
        SparkSession.builder.master("local").appName("biosales").getOrCreate(),
        get_pull_url_responses(urls=pull_urls))

    df.select("nome_item", "quantidade_item", "nome_regiao")\
        .write.partitionBy("nome_regiao").format("parquet").save(path_dataframe.format("parquet"))
    df.select("nome_item", "quantidade_item", "nome_regiao").coalesce(1) \
        .write.partitionBy("nome_regiao").format("csv").save(path_dataframe.format("csv"))
    df.coalesce(1).write.csv("biosales-by-region-sql")

    return df

# do not modify this method/function
if __name__ == '__main__':
    price_by_region_rdd()
    price_by_region_sql()
