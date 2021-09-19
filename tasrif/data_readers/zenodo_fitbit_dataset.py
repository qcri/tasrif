"""Module that provides classes to work with the fitbit dataset on the Zenodo platform
collected by crowd sourcing.

    Available Interday datasets:
        - ActivityDataset
        - WeightDataset
        - SleepDataset

    Available Intraday datasets:
        - IntradayCaloriesDataset
        - IntradayIntensitiesDataset
        - IntradayMETsDataset
        - IntradayStepsDataset


"""

import pathlib
import pandas as pd

from tasrif.processing_pipeline import (
    ProcessingOperator,
)

class ZenodoFitbitDataset(ProcessingOperator):
    """Base class for all Zenodo fitbit datasets
    """
    valid_table_names = [
        "Activity",
        "Weight",
        "Sleep",
        "IntradayCalories",
        "IntradayIntensities",
        "IntradayMETs",
        "IntradaySteps",
    ]

    def __init__(self, folder_path, table_name):
        """Initializes an interday dataset reader with the input parameters.

        Args:
            folder_path (str):
                Path to the Zenodo export folder_path containing data.
            table_name (str):
                The table to extract data from.
        """
        # Abort if table_name isn't valid
        super().__init__()
        self._validate_table_name(table_name)

        self.folder_path = folder_path
        self.table_name = table_name

    def process(self, *data_frames):
        # Accumulate dataframes from all files and concat them all at once
        dataframes = self._extract_data_from_file()
        return dataframes

    def _validate_table_name(self, table_name):
        """Validates table_name if it is included within the valid_table_names.

        Args:
            table_name (str):
                The table to validate.

        Raises:
            RuntimeError: Occurs when table_name is not included in self.valid_table_names

        """
        if table_name not in self.valid_table_names:
            raise RuntimeError(f"Invalid table_name, must be from the following: {self.valid_table_names}")

    def _extract_data_from_file(self):
        """Extracts the table data. Requires 'table_name' attribute to be set.

        Returns:
            pd.Dataframe
                Pandas dataframe object corresponding to the raw data.
        """

        dataframes = []
        subfolder_path_1 = "Fitabase Data 3.12.16-4.11.16"
        subfolder_path_2 = "Fitabase Data 4.12.16-5.12.16"
        full_path_1 = pathlib.Path(self.folder_path, subfolder_path_1)
        full_path_2 = pathlib.Path(self.folder_path, subfolder_path_2)

        if isinstance(self.table_name, str):
            self.table_name = [self.table_name]

        for table_name in self.table_name:
            if table_name == "Activity":
                day_table1 = pathlib.Path(full_path_1, "dailyActivity_merged.csv")
                day_table2 = pathlib.Path(full_path_2, "dailyActivity_merged.csv")

            elif table_name == "Weight":
                day_table1 = pathlib.Path(full_path_1, "weightLogInfo_merged.csv")
                day_table2 = pathlib.Path(full_path_2, "weightLogInfo_merged.csv")

            elif table_name == "Sleep":
                day_table1 = pathlib.Path(full_path_1, "minuteSleep_merged.csv")
                day_table2 = pathlib.Path(full_path_2, "minuteSleep_merged.csv")

            elif table_name == "IntradayCalories":
                day_table1 = pathlib.Path(full_path_1, "minuteCaloriesNarrow_merged.csv")
                day_table2 = pathlib.Path(full_path_2, "minuteCaloriesNarrow_merged.csv")

            elif table_name == "IntradayIntensities":
                day_table1 = pathlib.Path(full_path_1, "minuteIntensitiesNarrow_merged.csv")
                day_table2 = pathlib.Path(full_path_2, "minuteIntensitiesNarrow_merged.csv")

            elif table_name == "IntradayMETs":
                day_table1 = pathlib.Path(full_path_1, "minuteMETsNarrow_merged.csv")
                day_table2 = pathlib.Path(full_path_2, "minuteMETsNarrow_merged.csv")

            elif table_name == "IntradaySteps":
                day_table1 = pathlib.Path(full_path_1, "minuteStepsNarrow_merged.csv")
                day_table2 = pathlib.Path(full_path_2, "minuteStepsNarrow_merged.csv")

            raw_df1 = pd.read_csv(day_table1)
            raw_df2 = pd.read_csv(day_table2)
            dataframe = pd.concat(
                            [raw_df1, raw_df2],
                            axis=0,
                            ignore_index=False,
                            keys=None,
                            levels=None,
                            names=None,
                            verify_integrity=False,
                            copy=True,
                        )

            dataframes.append(dataframe)

        return dataframes
