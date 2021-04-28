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

# %% [markdown]
# <a id='machine_learning'></a>
#
# ## Scenario 1
# ## Machine Learning for single CGM future prediction

# %%
# %load_ext autoreload
# %autoreload 2

from sklearn.model_selection import LeaveOneGroupOut, KFold, GroupKFold


from pycaret.regression import *

from Profast_ML_utils import *

from tqdm import tqdm

# %%
profast_datapath='../../data/profast2020/'

# %%
profast_ml = os.path.join(profast_datapath, "preprocessed")

df = pd.read_csv(os.path.join(profast_ml, "df_cleaned.csv.gz"))
add_time_related_features(df, "time")

df.head()

# %%
df_emr = pd.read_csv(os.path.join(profast_ml, "df_cleaned_emr.csv.gz"))
del df_emr["Diabetes Medication"]
print("EMR data for %d participants" % (df_emr.shape[0]))
df_emr.head()

# %% [markdown]
# # Feature extraction code

# %%
experiment_path = "/home/palotti/github/tasrif/data/profast2020/scenario1/"

# %%
signals = ["time", "patientID", "HeartRate", "mets", "Calories", "Steps", "Distance", "CGM"]

winsizes = ["5h", "3h", "4h", "6h"]
deltas = ["5h", "1h", "2h", "3h", "4h", "6h", "7h", "8h"]

params = []
for winsize in winsizes:
    for delta in deltas:
        params.append([winsize, delta])
            
print("Processing %d param combinations." % (len(params)))

for param in tqdm(params):
    
    winsize, delta = param
    
    if data_exists(experiment_path, winsize, delta):
        print("%s: %s, %s was already processed" % (experiment_path, winsize, delta))
        continue

    df_timeseries, df_labels, df_label_time, df_pids = generate_timeseries_df(df, signals, winsize, delta)

    features_filtered = extract_features_from_df(experiment_path, df_timeseries,
                                                 df_labels, 
                                                 winsize,
                                                 delta)

    data = pd.concat([df_pids, df_labels, df_label_time, features_filtered], axis=1)
        
    time_features = [k for k in df.keys() if k.startswith("time")]
    df_ehr_time = pd.merge(df[["patientID", *time_features]], df_emr)
    
    data = pd.merge(df_ehr_time, data,
                    left_on=["patientID", "time"], right_on=["pid", "gt_time"])
    data = data.drop(columns=["patientID","time"])
    
    print("Note that there were many NAN in the ERM files.")
    print("If we dropall NAN, we go from %d rows to %d (i.e., from %d to %d participants)" % (data.shape[0], 
                                                                                                  data.dropna().shape[0],
                                                                                                  data["pid"].unique().shape[0],
                                                                                                  data.dropna()["pid"].unique().shape[0]))

    print("So, lets dropping NAs...")
    data = data.dropna()
    save_data(experiment_path, data, winsize, delta)


# %% [markdown]
# # Understanding the generated data

# %%
df_result = []
delta = "0h"
for winsize in winsizes:
    for win_of_interest in wins_of_interest:
        data = load_data(winsize, delta, win_of_interest)
        row = {}
        row["win_of_interest"] = win_of_interest
        row["delta"] = delta
        row["winsize"] = winsize
        row["total_number_examples"] = data.shape[0]
        df_result.append(row)
    
pd.DataFrame(df_result).pivot(index="winsize", values="total_number_examples", columns="win_of_interest").plot()

# %% [markdown]
# # ML Task

# %%

winsize = "5h"
delta = "1h"

featset = ['time', 'HeartRate', 'mets', 'Calories', 'Steps', 'Distance', 'CGM']

data, feature_mapping = prepare_ml(winsize, delta, win_of_interest="0h", signals)

train_data, test_data = split_train_test(data, featset, feature_mapping)
    
    
cv = GroupKFold() #### <---- This is extremely important to avoid auto-correlation between the train/test

experiment = setup(data = train_data, test_data = test_data,
                   target='ground_truth', session_id=42, silent=True,
                   fold_strategy = cv, fold_groups = 'fold',
                   ignore_features = ["pid", "fold"]
                  )
best_model = compare_models()


# %%
predict_model(best_model)

# %%
plot_model(best_model, "feature")
