"""
How to use:
python3 read_json.py \
--input_path 'Data/2019ProductList.json' \
--output_path 'Result/2019ProductList' \
--fields_metadata '[
    {"field_name": "ProductID",
     "data_type": "string"},
    {"field_name": "ListPrice",
     "data_type": "float",
     "new_field_name": "List_Price_USD"}
]'


python3 read_json.py \
    --fields_metadata '[
        {"field_name": "ListPrice",
         "data_type": "float",
         "new_field_name": "List_Price_USD"},
        {"field_name": "Weight",
         "data_type": "float",
         "new_field_name": "Weight_Kg"}
    ]' \
    --input_options '{"multiline": "true"}' \
    --input_path 'Data/2020ProductList.json' \
    --output_path 'Results/2020ProductList' \
    --null_value 'N'


python3 read_json.py \
    --fields_metadata '[
        {"field_name": "ListPrice",
         "data_type": "float",
         "new_field_name": "List_Price_USD"},
        {"field_name": "StandardCost",
         "data_type": "float",
         "new_field_name": "Standard_Cost_USD"},
        {"field_name": "ModifiedDate",
         "data_type": "timestamp"}
    ]' \
    --input_path 'Data/2021ProductList.json' \
    --output_path 'Results/2021ProductList'

"""

import argparse
import json
from typing import List
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F


def parse_args():
    parser = argparse.ArgumentParser(description='Script to transform a given DF')
    parser.add_argument('--input_path', required=True)
    parser.add_argument('--input_options', required=False, default='{}')
    parser.add_argument('--fields_metadata', required=False, default='{}')
    parser.add_argument('--output_format', required=False, default='parquet')
    parser.add_argument('--null_value', required=False, default='\\N')
    parser.add_argument('--output_path', required=True)
    return parser.parse_args()


def transform_df(df: DataFrame, fields_metadata: List, null_value: str = '\\N') -> DataFrame:
    fields_names, fields_dtypes = [list(row) for row in zip(*df.dtypes)]
    for field_metadata in fields_metadata:
        original_name_indx = fields_names.index(field_metadata["field_name"])
        _new_dtype = field_metadata.get("data_type")
        _new_name = field_metadata.get("new_field_name")
        fields_names[original_name_indx] = _new_name if _new_name else fields_names[original_name_indx]
        fields_dtypes[original_name_indx] = _new_dtype if _new_dtype else fields_dtypes[original_name_indx]
    df_transformed = df.select(
        *[F.col(original_name).alias(new_name).cast(field_dtype)
          for original_name, new_name, field_dtype in zip(df.columns, fields_names, fields_dtypes)])
    return df_transformed.replace({null_value: None})


if __name__ == '__main__':
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.options(**json.loads(args.input_options)).json(args.input_path)
    dft = transform_df(df, json.loads(args.fields_metadata), args.null_value)
    df.printSchema()
    dft.printSchema()
    df.show(50, False)
    dft.show(50, False)
    dft.write.format(args.output_format).save(args.output_path, mode='overwrite')
    spark.stop()



# df
# # +----------------------------+
# # |col_name                    |
# # +----------------------------+
# # |Sun,24 Oct 2021 18:51:51 GMT|
# # |Sun,24 Oct 2021 18:51:52 GMT|
# # |Sun,24 Oct 2021 18:51:53 GMT|
# # |Sun,24 Oct 2021 18:51:52 GMT|
# # |Sun,24 Oct 2021 18:51:51 GMT|
# # +----------------------------+
#
# frmt='(\w{3}\,)(\d{2}\s\w{3}\s\d{4}\s)(\d{2}:\d{2}:\d{2})(\s\w*)'
# df.select(F.regexp_replace('col_name', frmt, '$2$4').alias('result'))
# # +----------------+
# # |result          |
# # +----------------+
# # |24 Oct 2021  GMT|
# # |24 Oct 2021  GMT|
# # |24 Oct 2021  GMT|
# # |24 Oct 2021  GMT|
# # |24 Oct 2021  GMT|
# # +----------------+