"""
Module that provides mixins for commonly-used validations.
"""
import pandas as pd

class ValidationError(Exception):
    """
    Raise when a validation fails in the validate hook of an Operator.
    """

#pylint: disable=too-few-public-methods
class InputsAreDataFramesValidatorMixin:
    """
    Validates whether the inputs to the operator are DataFrames.
    """
    def _validate(self, *data_frames): #pylint: disable=no-self-use
        inputs_are_data_frames = [isinstance(df, pd.DataFrame) for df in data_frames]

        if not all(inputs_are_data_frames):
            raise ValidationError("Some inputs are not pandas.DataFrames!")
