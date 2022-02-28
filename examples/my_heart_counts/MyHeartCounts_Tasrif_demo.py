# # Goal of the notebook
# End to end example to preprocess MyHeartCounts dataset using Tasrif

# +
import os

from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVR

from tasrif.data_readers.my_heart_counts import MyHeartCountsDataset
from tasrif.processing_pipeline import (
    ComposeOperator,
    MapProcessingOperator,
    SequenceOperator,
)
from tasrif.processing_pipeline.custom import (
    AggregateOperator,
    FilterOperator,
    ReadCsvFolderOperator,
    ResampleOperator,
    SetStartHourOfDayOperator,
    SlidingWindowOperator,
)
from tasrif.processing_pipeline.observers import VisualizeDaysObserver
from tasrif.processing_pipeline.pandas import (
    ConcatOperator,
    ConvertToDatetimeOperator,
    DropNAOperator,
    GroupbyOperator,
    RenameOperator,
)
from tasrif.processing_pipeline.tsfresh import TSFreshFeatureExtractorOperator

# Can we predict the distance a user would do in the next timestep (e.g. hour) using past HR and Steps
# Set the below line to MyHeartCounts dataset path
mhc_folder_path = os.environ["MYHEARTCOUNTS"]

# Read myHeartCounts
mhc = MyHeartCountsDataset(
    path_name=mhc_folder_path,
    table_name="healthkitdata",
    participants=10,
    sources=["phone"],
    types=["HKQuantityTypeIdentifierDistanceWalkingRunning"],
    split=True,
)

df = mhc.process()

# Summary statistics are easy to calculate
op = AggregateOperator(
    groupby_feature_names=["recordId", "type"],
    aggregation_definition={"value": ["sum", "mean"]},
)


print(op.process(*df))


# We can perform a few data preprocessing and filtering operators to clean the data
# We are plotting for the first participant in the dataframe (the default)
observer = VisualizeDaysObserver(
    date_feature_name="startTime",
    signals=["value"],
    participant_identifier="recordId",
    start_hour_col="shifted_time_col",
    end_date_feature_name="endTime",
    log_scale=False,
    figsize=(14, 7),
)

preprocess_pipeline = SequenceOperator(
    [
        SetStartHourOfDayOperator(
            date_feature_name="startTime",
            participant_identifier="recordId",
            shifted_date_feature_name="shifted_time_col",
            shift=9,
        ),
        # Filter data to include days with > 2500 steps
        FilterOperator(
            participant_identifier="recordId",
            date_feature_name="shifted_time_col",
            day_filter={
                "column": "value",
                "filter": lambda value: value.sum() > 2500,
            },
            filter_type="include",
        ),
    ],
    observers=[observer],
)

processed_df = preprocess_pipeline.process(*df)

# +
# Create windows from time series
# The windows indices will be labeled in the dataframe
swo = SlidingWindowOperator(
    winsize="1h15t",
    time_col="startTime",
    label_col="value",
    participant_identifier="recordId",
)


# Return the labeled dataframe and
# the true labels (y) that are 15 minutes following the training window size (1 hour)
df_timeseries, y, _, _ = swo.process(*processed_df)[0]

# Define pipeline that uses TSFresh package for the feature extraction
feature_extraction_pipeline = SequenceOperator(
    [
        TSFreshFeatureExtractorOperator(
            date_feature_name="startTime", value_col="value", labels=y
        ),
        DropNAOperator(axis=1),  # Drop null extracted features
    ]
)

# Extract features from timeseries and name them X
X = feature_extraction_pipeline.process(df_timeseries)[0]

# Use your model to train on your preprocessed data
pipe = Pipeline([("scaler", StandardScaler()), ("svc", SVR(kernel="linear"))])
pipe.fit(X, y)
pipe.score(X, y)
