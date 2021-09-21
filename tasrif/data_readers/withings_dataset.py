"""
Module that provides classes to work with a exported withings dataset
    **Available datasets**:
        - Activities
        - Sleep and Sleep State
        - Physical Activity (distance, calories, heart-rate, steps)
        - Blood Oxygen (SpO2)
        - Elevation, Horizontal and Vertical Radius
        - Blood Pressure
        - GPS (Lat, Long) and GPS Speed
        - Lap Pool
        - Quality Score
        - Altitude
        - Height and Weight

"""

import pathlib
import datetime
import re
import pandas as pd
from tasrif.processing_pipeline import ProcessingOperator


class WithingsDataset(ProcessingOperator):
    """Base class for all withings datasets
    """

    valid_table_names = ["Activities", "Sleep", "Steps", "Vertical_Radius", "Sleep_State",
                        "Quality_Score", "Lap_Pool", "Horizontal_Radius", "Gps_Speed", "Elevation",
                        "Distance", "Duration", "Calories", "SpO2", "Altitude", "Weight", "Height",
                        "Blood_Pressure", "Lat_Long"]

    def __init__(self, file_name, table_name):
        """Initializes a dataset reader with the input parameters.

        Args:
            file_name (str):
                Path to the withings export file containing data.
            table_name (str):
                The table to extract data from.

        """
        # Abort if table_name isn't valid
        super().__init__()
        self._validate_table_name(table_name)

        self.file_name = file_name
        self.table_name = table_name

    def process(self, *data_frames):

        if self.table_name in ["Lat_Long"]:

            with open(pathlib.Path(self.file_name[0]), "r") as file_object:
                file_object.seek(0)
                df_latitude = pd.read_csv(file_object)

            with open(pathlib.Path(self.file_name[1]), "r") as file_object:
                file_object.seek(0)
                df_longitude = pd.read_csv(file_object)

            df_latitude = self._expand_duration_to_times(
                df_latitude, 'latitude')

            df_longitude = self._expand_duration_to_times(
                df_longitude, 'longitude')

            longitude_values = df_longitude["longitude"]
            data_frame = df_latitude.join(longitude_values)

        else:
            with open(pathlib.Path(self.file_name), "r") as file_object:
                file_object.seek(0)
                data_frame = pd.read_csv(file_object)

        if self.table_name in ["Steps", "Vertical_Radius", "Sleep_State", "Quality_Score",
                               "Lap_Pool", "Horizontal_Radius", "Gps_Speed", "Elevation",
                               "Distance", "Duration", "Calories", "SpO2", "Altitude"]:
            table_name_case = re.sub(
            r'(?<!^)(?=[A-Z])', '_', self.table_name).lower()
            data_frame = self._expand_duration_to_times(
                data_frame, table_name_case)

        if self.table_name in ["Height"]:
            data_frame = self._convert_in_to_cm(data_frame, "Height (in)", "height")

        return [data_frame]

    def _validate_table_name(self, table_name):
        if table_name not in self.valid_table_names:
            raise RuntimeError(
                f"Invalid table_name, must be from the following: {self.valid_table_names}")

    @staticmethod
    def _expand_duration_to_times(data_frame, name):

        rows = []

        df_temp = data_frame.copy()

        # Cast value from string to list
        df_temp['value'] = df_temp.value.apply(lambda x: x[1:-1].split(','))

        # Cast duration from string to list of ints
        df_temp['duration'] = df_temp.duration.apply(
            lambda x: x[1:-1].split(','))
        df_temp['duration'] = df_temp.duration.apply(
            lambda x: [int(i) for i in x])

        df_temp.apply(lambda row: [rows.append([datetime.datetime.fromisoformat(row['start']) +
                                                datetime.timedelta(seconds=sum(row['duration'][0:index])),
                                                datetime.datetime.fromisoformat(row['start']) +
                                                datetime.timedelta(seconds=sum(row['duration'][0:index+1])),
                                                row['value'][index]])
                                   for index, end in enumerate(row['duration'])], axis=1)

        df_new = pd.DataFrame(rows, columns=['from', 'to', name])

        return df_new

    @staticmethod
    def _convert_in_to_cm(data_frame, old_column, new_column):

        reg_ex = re.compile(r"([0-9]+)' ([0-9]*\.?[0-9]+)")

        # Convert height unit from in to cm
        data_frame = data_frame.rename(columns={old_column: new_column})
        data_frame[new_column] = data_frame[new_column].apply(lambda x: float('NaN') if reg_ex.match(
            x) is None else 2.54*(int(reg_ex.match(x).group(1))*12 + float(reg_ex.match(x).group(2))))

        return data_frame
