from __future__ import print_function

import pandas as pd
from pyspark.ml.feature import OneHotEncoder
from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
from pandasticsearch import Select
from pyspark.ml.fpm import FPGrowth, FPGrowthModel
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer

es = Elasticsearch('http://localhost:9200')


def query_elasticsearch() -> pd.DataFrame:
    result_dict = es.search(index="fluentd-20191114", body={"query": {"match_all": {}}}, size=2200,
                            sort=["@timestamp:asc"], _source_include=["message", "@timestamp", "@log_name", "log",
                                                                      "pipeline_component", "level", "source", "type",
                                                                      "subject", "topic name", "asset_state",
                                                                      "function", "throwable"])
    data_frame = Select.from_dict(result_dict).to_pandas()
    data_frame = data_frame.drop(columns=['_index', '_type', '_id', '_score'])
    return data_frame


def perform_one_hot_encoding(data_frame: pd.DataFrame) -> pd.DataFrame:
    data_frame = data_frame.drop(columns=['@timestamp', 'message', 'log', 'subject'])
    data_frame.fillna('MISSING', inplace=True)
    column_transformer = ColumnTransformer([('encoder', OneHotEncoder(), [0, 1, 2, 3, 4, 5, 6, 7, 8])],
                                           remainder='passthrough')
    column_transformer.fit(data_frame)
    feat_names = column_transformer.get_feature_names()
    encoded_array = column_transformer.transform(data_frame).toarray()
    return pd.DataFrame(encoded_array, index=data_frame.index, columns=feat_names).astype(int)


def frequent_pattern_matching(encoded_data_frame: pd.DataFrame) -> FPGrowthModel:
    item_set = [[encoded_data_frame.columns.values[j]
                 for j in range(0, len(encoded_data_frame.iloc[i]))
                 if encoded_data_frame.iloc[i][j] == 1]
                for i in range(0, len(encoded_data_frame.index))]

    d = {'items': item_set}
    pandas_df = pd.DataFrame(d, columns=['items'])
    spark_df = spark.createDataFrame(pandas_df, schema=["items"])

    fp_growth = FPGrowth(minSupport=0.2, minConfidence=0.3)
    growth_model = fp_growth.fit(spark_df)
    growth_model.transform(spark_df).show(truncate=False)
    return growth_model


if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("spark://MacBook-Pro.local:7077") \
        .appName("OneHotEncoderExample") \
        .getOrCreate()

    # df = query_elasticsearch()
    df = pd.read_csv("./data/preprocessing/data_reduced.csv")
    df = perform_one_hot_encoding(df)

    df = df[df.columns.drop(list(df.filter(regex='MISSING')))]
    df.to_csv("./data/preprocessing/data_reduced_encoded_no_missing.csv", encoding='utf-8', index=False)

    model = frequent_pattern_matching(df)

    model.freqItemsets.show(n=50, truncate=False)
    model.associationRules.show(n=50, truncate=False)
    model.associationRules.toPandas().to_csv("./data/result/association_rules.csv", encoding='utf-8',
                                             index=False)
    model.freqItemsets.toPandas().to_csv("./data/result/frequent_item_sets.csv", encoding='utf-8',
                                         index=False)

    spark.stop()
