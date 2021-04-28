# ---
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: PyCharm (tasrif)
#     language: python
#     name: pycharm-5bd30262
# ---

# %%
import pandas as pd
import numpy as np
from tqdm import tqdm
import os

from tsfresh import extract_features
from tsfresh.utilities.dataframe_functions import impute
from tsfresh import extract_relevant_features

from sklearn.model_selection import LeaveOneGroupOut, KFold, GroupKFold



# %% [markdown]
# Time related features. Externaly, use ``add_time_related_features``

# %%
def convert_time_sin_cos(df, datetime_col):
    """
    The following idea comes from [this tensorflow tutorial on time_series analysis](https://www.tensorflow.org/tutorials/structured_data/time_series).
    Here we transform the time of the day into a continous sin/cos cyclic feature.
    """

    sec_in_day = 24*60*60
    
    ts = df[datetime_col].apply(lambda x: x.timestamp()).astype(int)
    day_sin = np.sin(ts * (2 * np.pi / sec_in_day))
    day_cos = np.cos(ts * (2 * np.pi / sec_in_day))
    
    return day_sin, day_cos
    
def ramadan_flag(t, start_ramadan=pd.Timestamp(2020, 4, 23, 0, 0, 0),
                    end_ramadan=pd.Timestamp(2020, 5, 23, 23, 59, 59)):

    if (t >= start_ramadan) & (t <= end_ramadan):
        return True
    else:
        return False
    
def add_ramadan_flags(df, datetime_col):

    if "Ramadan" in df:
        r = df["Ramadan"]
        del df["Ramadan"]
    else:
        # Add Ramadan and time flags
        r = df[datetime_col].apply(lambda x: ramadan_flag(x))
    return r
    
def add_weekend(df, time_col):
    return df[time_col].dt.weekday.isin([4,5]) # Monday is denoted by 0 and Sunday is denoted by 6.
    
def add_time_related_features(df, datetime_col):
    
    if datetime_col not in df:
        print("ERROR datetime_col %s not in df!" % (datetime_col))
        return

    if not pd.api.types.is_datetime64_ns_dtype(df[datetime_col]):
        df[datetime_col] = pd.to_datetime(df[datetime_col])
    
    df["time_sin"], df["time_cos"] = convert_time_sin_cos(df, datetime_col)
    df["time_ramadan"] = add_ramadan_flags(df, datetime_col)
   
    df["time_morning"] = df["time"].dt.hour.isin([6,7,8,9,10,11])
    df["time_afternoon"] = df["time"].dt.hour.isin([12,13,14,15,16,17,18])
    df["time_night"] = df["time"].dt.hour.isin([19,20,21,22,23])
    df["time_late_night"] = df["time"].dt.hour.isin([0,1,2,3,4,5])
    
    for period in ["time_morning", "time_afternoon", "time_night", "time_late_night"]:
        df[period + "_ramadan"] = df[period] & df["time_ramadan"]
    
    df["time_weekend"] = add_weekend(df, datetime_col)
    
    

# %%
def generate_slide_wins(df_in, start_seq=0, winsize="1h00t", delta="0h15t", 
                        time_col="time", 
                        label_col="CGM", pid_col="patientID"):
    """
    From a timeseries of ONE participant, this function generates two dataframes: <time_series_features>, <labels>
    The first dataframe can be used with tsfresh later on, while the second has all the labels that we want to predict.

    Notice that the default winsize is 1h and 15 minutres (`1h15t`).
    We used the first hour to extract the features and the 15 min only to collect the ground_truth labels.

    """
    seq_id = start_seq
    transformed_df = []
    list_of_indices = []
    labels = []
    label_times = []

    pid = df_in[pid_col].unique()
    if len(pid) > 1:
        print("ERROR: We should have only one pid here. Aborting")
        return
    pid = pid[0]
    
    df = df_in.reset_index(drop=True).copy()
    
    # The following code will construct a rolling win that could be based on either time or #win
    # This will feed list_of_indexes with the sub-win indices that will be used in the next for loop
    df.reset_index().rolling(winsize, on=time_col, center=False, closed="both")["index"].apply((lambda x: list_of_indices.append(x.tolist()) or 0))

    # Time-based win might be smaller than the expected size. 
    expected_size = (winsize // pd.Timedelta("15t")) + 1

    # Save a time indexed copy of the dataframe
    dftime = df.set_index(time_col).copy()
    
    for idx in list_of_indices:
        if len(idx) != expected_size:
            continue
            
        last_row = df.loc[idx].iloc[-1]
        label_time = last_row[time_col] + pd.Timedelta(delta)
        
        if label_time not in dftime.index:
            continue
        
        label = dftime.loc[label_time][label_col]
        
        # save instance
        labels.append(label)
        label_times.append(label_time)
        
        tmp_df = df.loc[idx].copy()
        tmp_df["seq_id"] = seq_id
        seq_id += 1

        del tmp_df[pid_col]
        
        transformed_df.append(tmp_df)

    labels = pd.Series(labels)
    labels.name = "ground_truth"
    
    label_times = pd.Series(label_times)
    label_times.name = "gt_time"
    
    transformed_df = pd.concat(transformed_df).reset_index(drop=True)
    pid = pd.Series([pid] * labels.shape[0])
    pid.name = "pid"
    
    return seq_id, transformed_df, labels, label_times, pid


def generate_timeseries_df(df, signals, winsize, delta, pid_col="patientID"):

    df_labels = []
    df_label_times = []
    df_timeseries = []
    df_pids = []

    last_seq_id = 0

    for pid in tqdm(df[pid_col].unique()):

        df_tmp = df[df[pid_col] == pid]

        last_seq_id, df_ts, df_label, df_label_time, df_pid = generate_slide_wins(df_tmp[signals],
                                                                                  start_seq=last_seq_id,
                                                                                  winsize=winsize,
                                                                                  delta=delta)
        df_timeseries.append(df_ts)
        df_labels.append(df_label)
        df_label_times.append(df_label_time)
        df_pids.append(df_pid)

    df_labels = pd.concat(df_labels).reset_index(drop=True)
    df_label_times = pd.concat(df_label_times).reset_index(drop=True)
    df_timeseries = pd.concat(df_timeseries).reset_index(drop=True)

    df_pids = pd.concat(df_pids).reset_index(drop=True)
    df_pids.name = "pid"
    
    return df_timeseries, df_labels, df_label_times, df_pids


# %%
def generate_slide_wins_from_block(df_in, start_seq=0, winsize="1h00t",
                                   delta="0h00t", win_of_interest="2h00t",
                                   time_col="time", 
                                   label_cols=["hyper", "hypo"], pid_col="patientID"):
    """
    From a timeseries of ONE participant, this function generates two dataframes: <time_series_features>, <labels>
    The first dataframe can be used with tsfresh later on, while the second has all the labels that we want to predict.

    Notice that the default winsize is 1h and 15 minutres (`1h15t`).
    We used the first hour to extract the features and the 15 min only to collect the ground_truth labels.

    """
    seq_id = start_seq
    transformed_df = []
    list_of_indices = []
    labels = []
    label_times = []

    pid = df_in[pid_col].unique()
    if len(pid) > 1:
        print("ERROR: We should have only one pid here. Aborting")
        return
    pid = pid[0]
    
    df = df_in.reset_index(drop=True).copy()
    
    # The following code will construct a rolling win that could be based on either time or #win
    # This will feed list_of_indexes with the sub-win indices that will be used in the next for loop
    df.reset_index().rolling(winsize, on=time_col, center=False, closed="both")["index"].apply((lambda x: list_of_indices.append(x.tolist()) or 0))

    # Time-based win might be smaller than the expected size. 
    expected_size = (winsize // pd.Timedelta("15t")) + 1

    # Save a time indexed copy of the dataframe
    dftime = df.set_index(time_col).copy()
    
    # We need to shift the data first and then roll and sum each part
    # This operation will give us the sum of the label in the window of size `win_of_interest` from a given timestapm
    
    labels_of_interest = []
    for label_col in label_cols:
        label_of_interest = label_col + "_" + win_of_interest
        print("label of interest", label_of_interest)
        dftime[label_of_interest] = dftime[label_col].shift(freq="-%s" % (win_of_interest))
        dftime[label_of_interest] = dftime[label_of_interest].fillna(0.0).rolling(win_of_interest, center=False, closed="both").sum(skipna=True)
        labels_of_interest.append(label_of_interest)

    for idx in list_of_indices:
        if len(idx) != expected_size:
            continue
            
        last_row = df.loc[idx].iloc[-1]
        label_time = last_row[time_col] + pd.Timedelta(delta)
        
        if label_time not in dftime.index:
            continue
        
        label = dftime.loc[label_time][labels_of_interest].values
        
        # save instance
        labels.append(label)
        label_times.append(label_time)
        
        tmp_df = df.loc[idx].copy()
        tmp_df["seq_id"] = seq_id
        seq_id += 1

        del tmp_df[pid_col]
        
        transformed_df.append(tmp_df)

    labels = pd.DataFrame(labels, columns=label_cols)
    labels.name = "ground_truth"
    
    label_times = pd.Series(label_times)
    label_times.name = "gt_time"
    
    transformed_df = pd.concat(transformed_df).reset_index(drop=True)
    pid = pd.Series([pid] * labels.shape[0])
    pid.name = "pid"
    
    return seq_id, transformed_df, labels, label_times, pid


def generate_timeseries_df_from_block(df, signals, winsize, delta, win_of_interest, pid_col="patientID",
                                      label_cols=["hyper", "hypo"]):

    df_labels = []
    df_label_times = []
    df_timeseries = []
    df_pids = []

    last_seq_id = 0
    
    signals += label_cols

    for pid in tqdm(df[pid_col].unique()):

        print("PID:", pid)
        df_tmp = df[df[pid_col] == pid]

        last_seq_id, df_ts, df_label, df_label_time, df_pid = generate_slide_wins_from_block(df_tmp[signals],
                                                                                             label_cols=label_cols,
                                                                                             start_seq=last_seq_id,
                                                                                             winsize=winsize,
                                                                                             delta=delta,
                                                                                             win_of_interest=win_of_interest)
        df_timeseries.append(df_ts)
        df_labels.append(df_label)
        df_label_times.append(df_label_time)
        df_pids.append(df_pid)

    df_labels = pd.concat(df_labels).reset_index(drop=True)
    df_label_times = pd.concat(df_label_times).reset_index(drop=True)
    df_timeseries = pd.concat(df_timeseries).reset_index(drop=True)

    df_pids = pd.concat(df_pids).reset_index(drop=True)
    df_pids.name = "pid"
    
    for col in label_cols:
        del df_timeseries[col]
    
    return df_timeseries, df_labels, df_label_times, df_pids


# %%
def extract_features_from_df(experiment_path, df_timeseries, df_labels, winsize, delta, win_of_interest="0h"):
    
    output_filename = os.path.join(experiment_path, "filtered_features_%s_d%s_wi%s.csv.gz" % (winsize, delta, win_of_interest))
    
    if os.path.exists(output_filename):
        features_filtered = pd.read_csv(output_filename)
    else:
        features_filtered = extract_relevant_features(df_timeseries, df_labels, column_id='seq_id', column_sort='time')
        features_filtered.to_csv(output_filename, index=False)
    
    return features_filtered



# %% [markdown]
# TSFresh is an expensive procedure. We use the following methods to save/load the processed data.

# %%
def get_filename(experiment_path, winsize, delta, win_of_interest="0h"):
    filename = os.path.join(experiment_path, "data_%s_d%s_wi%s.csv.gz" % (winsize, delta, win_of_interest))
    return filename

def save_data(experiment_path, df, winsize, delta, win_of_interest="0h"):
    output_filename = os.path.join(experiment_path, "data_%s_d%s_wi%s.csv.gz" % (winsize, delta, win_of_interest))
    df.to_csv(output_filename, index=False)
    
def load_data(experiment_path, winsize, delta, win_of_interest="0h"):
    filename = get_filename(experiment_path, winsize, delta, win_of_interest)
    return pd.read_csv(filename)

def data_exists(experiment_path, winsize, delta, win_of_interest="0h"):
    filename = get_filename(experiment_path, winsize, delta, win_of_interest)
    print("Filename:", filename)
    return os.path.exists(filename)



# %%
def get_feature_mapping(signals, data):

    feature_mapping = {}
    mapped_feature = set([])

    for feature in signals:
        for k in data.keys():
            if k.startswith(feature):
                if feature not in feature_mapping:
                    feature_mapping[feature] = []
                feature_mapping[feature].append(k)            
                mapped_feature.add(k)

    feature_mapping["other"] = []
    for k in data.keys():
        if k not in mapped_feature and k not in ['pid', 'ground_truth', 'gt_time']:
            feature_mapping["other"].append(k)

    return feature_mapping


# %%
def map_id_fold(all_ids, n, pid_col="pid"):
    
    pids = all_ids[pid_col].unique().ravel()
    if n < 0:
        n = len(pids)
    
    kf = KFold(n_splits=n, shuffle=True, random_state=42)
    mapping = []
    for i, (_, test) in enumerate(kf.split(pids)):
        for pid_index in test:
            mapping.append({'fold': i, pid_col: pids[pid_index]})

    return pd.DataFrame(mapping)


# %%
def prepare_ml(winsize, delta, win_of_interest, signals):
    data = load_data(experiment_path, winsize, delta, win_of_interest)
    
    df_folds = map_id_fold(data, -1)
    data = data.merge(df_folds)
    
    feature_mapping = get_feature_mapping(signals, data)
    
    return data, feature_mapping


def split_train_test(data, featset, feature_mapping):
    
    train_data = data[get_cols_by_featureset(data, featset, feature_mapping)]
    test_fold = train_data["fold"].max()
    test_data = train_data[train_data["fold"] == test_fold]
    train_data = train_data[train_data["fold"] != test_fold]

    return train_data, test_data


# %%
def classification_both_hyper_hypo(row):
    if is_hyper(cgm_value):
        return 1
    elif is_hypo(cgm_value):
        return 2
    return 0

def test_classification(gt_strategy, winsize, delta, win_of_interest, featset):
            
    data = load_data(winsize, delta, win_of_interest)
    
    if gt_strategy == "hyper":
        data["ground_truth"] = data["hyper"] > 0
    elif gt_strategy == "hypo":
        data["ground_truth"] = data["hypo"] > 0
    elif gt_strategy == "both":
        data["ground_truth"] = data[["hyper", "hypo"]].apply(lambda x: 0 if x["hyper"] == 0 and x["hypo"] == 0 else
                                                                       1 if x["hyper"] > 0 and x["hypo"] == 0 else
                                                                       2 if x["hypo"] > 0 and x["hyper"] == 0 else
                                                                       3, axis=1)
    
    feature_mapping = get_feature_mapping(featset, data)
    df_folds = map_id_fold(data, -1)
    data = data.merge(df_folds)

    train_data = data[get_cols_by_featureset(data, featset, feature_mapping)]
    test_fold = train_data["fold"].max()
    test_data = train_data[train_data["fold"] == test_fold]
    train_data = train_data[train_data["fold"] != test_fold]

    cv = GroupKFold() #### <---- This is extremely important to avoid auto-correlation between the train/test

    experiment = setup(data = train_data, test_data = test_data,
                   target='ground_truth', session_id=42, silent=True,
                   fold_strategy = cv, fold_groups = 'fold',
                   ignore_features = ["pid", "time", "fold", "Diabetes Medication", "hyper", "hypo"]
                  )
    
    best_model = compare_models(include=['lightgbm', 'lr', 'et'], n_select=1)
    best_model = create_model(best_model)
    
    #best_model = create_model("lightgbm")
    train_result = pull()
    prediction_results = predict_model(best_model)
    test_result = pull()
    
    return train_result, test_result

def get_cols_by_featureset(data, featset, feature_mapping):
    acc_feats = []
    for f in featset:
        acc_feats.extend(feature_mapping[f])
        
    return ["pid", "ground_truth", "fold"] + acc_feats

def get_classification_results_from_regression(df_test, cgm_gt="ground_truth", cgm_predicted="Label"):
    
    df = df_test.copy()
    
    df["class_gt"] = df[cgm_gt].apply(lambda x: hyper_hypo_label(x))
    df["class_predicted"] = df[cgm_predicted].apply(lambda x: hyper_hypo_label(x))

    cf_matrix = metrics.confusion_matrix(df["class_gt"], df["class_predicted"])
    cf_matrix = pd.DataFrame(cf_matrix, index=["Normal", "Hyper", "Hypo"], columns=["Normal", "Hyper", "Hypo"])
    fig = sns.heatmap(cf_matrix, annot=True, fmt='d', cmap='Blues')
    
    f1_mac = metrics.f1_score(df["class_gt"], df["class_predicted"], average="macro")
    f1_mic = metrics.f1_score(df["class_gt"], df["class_predicted"], average="micro")
    prec = metrics.precision_score(df["class_gt"], df["class_predicted"], average="weighted")
    recall = metrics.recall_score(df["class_gt"], df["class_predicted"], average="weighted")
    mcc = metrics.matthews_corrcoef(df["class_gt"], df["class_predicted"])
    
    
    return f1_mac, f1_mic, prec, recall, mcc, fig
