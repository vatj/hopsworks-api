#
#   Copyright 2024 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
from __future__ import annotations

import math
import re
import warnings
from typing import TYPE_CHECKING, List, Union

from hsfs.core.constants import HAS_GREAT_EXPECTATIONS


if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    import pyspark.sql.DataFrame
    from hsfs import ExternalFeatureGroup, FeatureGroup
    from hsfs import feature as feature_mod


REGEX_COMPILED_INTEGER_PATTERN = re.compile(r"\d+")


def dataframe_schema_validation(
    fgroup_obj: Union[FeatureGroup, ExternalFeatureGroup],
    dataframe: Union[pd.DataFrame, pl.DataFrame, pyspark.sql.DataFrame],
):
    if HAS_GREAT_EXPECTATIONS:
        # The schema will be validated when running Great Expectations
        return
    else:
        manual_schema_validation(fgroup_obj, dataframe)


def manual_schema_validation(
    fgroup_obj: Union[FeatureGroup, ExternalFeatureGroup],
    dataframe: Union[pd.DataFrame, pl.DataFrame, pyspark.sql.DataFrame],
):
    no_nulls_in_primary_key(fgroup_obj.primary_key, dataframe)

    check_str_lengths(fgroup_obj.features, dataframe)


def no_nulls_in_primary_key(
    primary_keys: List[str],
    dataframe: Union[pd.DataFrame, pl.DataFrame, pyspark.sql.DataFrame],
):
    if hasattr(dataframe, "dtype"):
        # Pandas
        na_null_pk = dataframe[primary_keys].isnull().any(axis=1)
    elif hasattr(dataframe, "schema"):
        # polars
        na_null_pk = dataframe.select(primary_keys).is_na().any(axis=1)
    else:
        # Spark
        na_null_pk = dataframe.select(primary_keys).isNull().any(axis=1)

    if na_null_pk:
        raise ValueError(
            f"Found null values in primary key column(s) {primary_keys} in dataframe."
            "Row which have null values in primary key columns cannot be inserted in online feature store."
            "Either drop the corresponding rows or disable the primary key check by setting run_validation=False."
        )


def check_str_lengths(
    features: List[feature_mod.Feature],
    dataframe: Union[pd.DataFrame, pl.DataFrame, pyspark.sql.DataFrame],
    adjust: bool = False,
):
    varchar_features = filter(lambda x: x.onlineType.startswith("varchar"), features)
    error_columns = {}
    for feat in varchar_features:
        # Current max allowed byte length of the column
        online_size = int(
            re.finditer(REGEX_COMPILED_INTEGER_PATTERN, feat.onlineType).next()
        )
        if hasattr(dataframe, "schema"):
            # Polars
            max_length = dataframe[feat.name].str.len_bytes().max()
        elif hasattr(dataframe, "dtypes"):
            dataframe[feat].astype(str).str.encode().len().max()
        else:
            # Spark
            max_length = (
                dataframe.select(feat.name)
                .rdd.map(lambda x: len(str(x[0].encode("utf-8"))))
                .max()
            )

        if max_length > online_size:
            error_columns[feat.name] = {
                "online_size": online_size,
                "max_length": max_length,
            }
            if adjust:
                # round up to nearest 1000
                new_size = math.ceil(max_length, -3)
                feat.onlineType = f"varchar({new_size})"
                error_columns[feat.name]["new_online_type"] = feat.onlineType

    err_msg = ""
    for name, values in error_columns.items():
        if adjust:
            warnings.warn(
                f"Found max byte length of string in column {name} to be "
                f"{values['max_length']}, adjusting online_store column size "
                f"to {values['new_online_type']}.",
                stacklevel=2,
            )
        else:
            err_msg += (
                f"Found max byte length of string in column {name} to be "
                f"{values['max_length']}, aborting insert. "
            )

        if not adjust:
            raise ValueError(err_msg)
