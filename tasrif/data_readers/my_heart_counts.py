"""
Module that provides class to work with the MyHeartCounts dataset.
"""
from tasrif.processing_pipeline.pandas import ReadCsvOperator

class MyHeartCountsDataset(ReadCsvOperator):
    """
    Class to work with the MyHeartCounts dataset.
    """
    day_one_survey_device_mapping = {
        "iPhone": "1",
        "ActivityBand": "2",
        "Pedometer": "2",
        "SmartWatch": "3",
        "AppleWatch": "3",
        "Other": "Other",
    }
