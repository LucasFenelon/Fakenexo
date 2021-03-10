from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from biosales_service import get_rdd_qt_avg_by_region_item, get_df_qt_avg_by_region_item
import biosales

class TestClass(object):
    def test_results_count(self):
        rdd = get_rdd_qt_avg_by_region_item(
            SparkSession.builder.master("local").appName("biosales").getOrCreate(),
            biosales.get_pull_url_responses(urls=biosales.pull_urls))
        df = get_df_qt_avg_by_region_item(
            SparkSession.builder.master("local").appName("biosales").getOrCreate(),
            biosales.get_pull_url_responses(urls=biosales.pull_urls))
        assert df.select("nome_regiao", "nome_item", "quantidade_item").count() == \
               rdd.toDF.select("_3", "_1", "_2").count()

    def test_results_values(self):
        rdd = get_rdd_qt_avg_by_region_item(
            SparkSession.builder.master("local").appName("biosales").getOrCreate(),
            biosales.get_pull_url_responses(urls=biosales.pull_urls))
        df = get_df_qt_avg_by_region_item(
            SparkSession.builder.master("local").appName("biosales").getOrCreate(),
            biosales.get_pull_url_responses(urls=biosales.pull_urls))
        assert df.select("nome_regiao", "nome_item", "quantidade_item") \
                .groupBy("nome_regiao", "nome_item").agg(sum("quantidade_item")).collect() == \
            rdd.toDF.select("_3", "_1", "_2") \
                .groupBy("_3", "_1").agg(sum("_2")).collect()