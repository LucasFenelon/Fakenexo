from pyspark.sql.functions import *
from biosales_dataframe import get_df_from_csv
from biosales_rdd import get_rdd_from_csv
from biosales_dao import join_df_biosales_item, join_df_biosales_region

def get_rdd_qt_avg_by_region_item(spark, response_content):
    rdd_biosales = get_rdd_from_csv(spark=spark, response_csv=response_content[0])
    #rdd_biosales_buyers = get_rdd_from_csv(spark=spark, response_csv=response_content[1]) \
    #    .map(lambda bio: (bio.id_regiao, bio.nome_regiao))
    #rdd_biosales_sellers = get_rdd_from_csv(spark=spark, response_csv=response_content[2]) \
    #    .map(lambda bio: (bio.id_regiao, bio.nome_regiao))
    rdd_biosales_regions = get_rdd_from_csv(spark=spark, response_csv=response_content[3]) \
        .map(lambda bio: (bio.id_regiao, bio.nome_regiao))
    rdd_biosales_item = get_rdd_from_csv(spark=spark, response_csv=response_content[4]) \
        .map(lambda bio: (bio.id_item, bio.nome_item))
    #rdd_biosales_platforms = get_rdd_from_csv(spark=spark, response_csv=response_content[5]) \
    #    .map(lambda bio: (bio.id_item, bio.nome_item))

    rdd_biosales_by_region = rdd_biosales \
        .map(lambda bio: (bio.id_regiao_comprador, (bio.id_item, bio.quantidade_item))) \
        .filter(lambda bio: bio[0] is not None)
    rdd_join_regions = rdd_biosales_by_region.join(rdd_biosales_regions)
    rdd_regions_adjust = rdd_join_regions \
        .map(lambda bio: (bio[1][0][0], (bio[1][0][1], bio[1][1])))
    rdd_join_item = rdd_regions_adjust.join(rdd_biosales_item)
    rdd_item_adjust = rdd_join_item \
        .map(lambda bio: ((bio[1][0][1], bio[1][1]), int(bio[1][0][0]))) \
        .filter(lambda bio: int(bio[1]) >= 0)

    rdd = rdd_item_adjust.reduceByKey(lambda a, b: a + b)
    rdd_count = rdd_item_adjust.map(lambda bio: (bio[0], 1)) \
        .reduceByKey(lambda a, b: a + b)
    rdd_join = rdd.join(rdd_count)

    rdd_avg = rdd_join.map(lambda bio: (bio[0], (float(bio[1][0]) / float(bio[1][1]))))
    rdd_final = rdd_avg.map(lambda bio: (bio[0][1], bio[1], bio[0][0]))

    return rdd_final


def get_df_qt_avg_by_region_item(spark, response_content):
    df_biosales = get_df_from_csv(spark=spark, response_csv=response_content[0])
    #df_biosales_buyers = get_df_from_csv(spark=spark, response_csv=response_content[1])
    #df_biosales_sellers = get_df_from_csv(spark=spark, response_csv=response_content[2])
    df_biosales_regions = get_df_from_csv(spark=spark, response_csv=response_content[3])
    df_biosales_items = get_df_from_csv(spark=spark, response_csv=response_content[4])
    #df_biosales_platforms = get_df_from_csv(spark=spark, response_csv=response_content[5])

    df_join_item = join_df_biosales_item(df=df_biosales, df_item=df_biosales_items)
    df_join_region = join_df_biosales_region(df=df_join_item, df_region=df_biosales_regions)
    df = df_join_region.filter(col("nome_regiao").isNotNull()).filter(col("quantidade_item") >= lit("0"))
    df_final = df.select("nome_regiao", "nome_item", "quantidade_item") \
        .groupBy("nome_regiao", "nome_item").agg(avg("quantidade_item").alias("quantidade_item"))

    return df_final