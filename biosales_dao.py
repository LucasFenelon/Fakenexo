
def join_df_biosales_item(df, df_item):
    return df.join(df_item, df["id_item"] == df_item["id_item"])

def join_df_biosales_region(df, df_region):
    return df.join(df_region, df["id_regiao_comprador"] == df_region["id_regiao"])