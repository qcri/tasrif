"""Example that represents the sleep related CSV files of the fitbit dataset published on Zenodo"""
import os
import pandas as pd

from tasrif.data_readers.zenodo_fitbit_dataset import ZenodoFitbitDataset
from tasrif.processing_pipeline import ComposeOperator, NoopOperator, SequenceOperator
from tasrif.processing_pipeline.custom import (
    AddDurationOperator,
    AggregateOperator,
    CreateFeatureOperator,
)
from tasrif.processing_pipeline.pandas import FillNAOperator

zenodo_folder_path = os.environ.get("ZENODOFITBIT_PATH")

DAILY_AGGREGATION_DEFINITION = {
    "duration": ["sum"],
    "date": ["first"],
    "value": ["mean"],
}

TOTAL_AGGREGATION_DEFINITION = {
    "logId": ["count"],
    "total_sleep_seconds": ["mean", "std"],
    "value_mean": ["mean", "std"],
}

pipeline = SequenceOperator(
    [
        ZenodoFitbitDataset(zenodo_folder_path, table_name="Sleep"),
        CreateFeatureOperator(
            feature_name="date",
            feature_creator=lambda df: pd.to_datetime(df["date"]),
        ),
        FillNAOperator(value={'index': df['date'].min()}),
        AddDurationOperator(groupby_feature_names="logId", date_feature_name="date"),
        AggregateOperator(
            groupby_feature_names=["logId", "Id"],
            aggregation_definition=DAILY_AGGREGATION_DEFINITION,
        ),
        ComposeOperator(
            [
                NoopOperator(),
                ComposeOperator(
                    [
                        CreateFeatureOperator(
                            feature_name="total_sleep_seconds",
                            feature_creator=lambda df: df.duration_sum.dt.total_seconds(),
                        ),
                        AggregateOperator(
                            groupby_feature_names="Id",
                            aggregation_definition=TOTAL_AGGREGATION_DEFINITION,
                        ),
                    ]
                ),
            ]
        ),
    ]
)

df = pipeline.process()

print(df)
