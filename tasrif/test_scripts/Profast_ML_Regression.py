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
# ## Traditional Machine Learning for single CGM future prediction

# %%
# %load_ext autoreload
# %autoreload 2

from sklearn.model_selection import LeaveOneGroupOut, KFold, GroupKFold

from pycaret.regression import *

from Profast_ML_utils import *

from tqdm import tqdm

import seaborn as sns
import matplotlib.pyplot as plt
sns.set_theme(style="darkgrid")
sns.set(rc={'figure.figsize':(11.7,8.27)})

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
experiment_path = os.path.join(profast_datapath, "scenario1/")

# %%
signals = ["time", "patientID", "HeartRate", "mets", "Calories", "Steps", "Distance", "CGM"]

winsizes = ["5h", "3h"] # , "4h", "6h"]
deltas = ["1h"] # , "1h", "2h", "3h", "4h", "6h", "7h", "8h"]

params = []
for winsize in winsizes:
    for delta in deltas:
        params.append([winsize, delta])
            
print("Processing %d param combinations." % (len(params)))

time_features = [k for k in df.keys() if k.startswith("time")]
df_ehr_time = pd.merge(df[["patientID", *time_features]], df_emr)

for param in tqdm(params):
    
    winsize, delta = param
    
    if data_exists(experiment_path, "tsfresh", winsize, delta) and data_exists(experiment_path, "raw", winsize, delta):
        print("%s: %s, %s was already processed" % (experiment_path, winsize, delta))
        continue

    df_timeseries, df_labels, df_label_time, df_pids = generate_timeseries_df(df, signals, winsize, delta)

    if not data_exists(experiment_path, "raw", winsize, delta):
        raw_features = calculate_raw_features(df_timeseries, ["CGM", "HeartRate", "mets", "Calories", "Steps", "Distance"], "seq_id")
        raw_data = pd.concat([df_pids, df_labels, df_label_time, raw_features], axis=1)
        raw_data = merge_ehr_features(df_ehr_time, raw_data, dropna=True)
        save_data(experiment_path, "raw", raw_data, winsize, delta)
    
    if not data_exists(experiment_path, "tsfresh", winsize, delta):
        features_filtered = extract_features_from_df(experiment_path, df_timeseries,
                                                 df_labels, 
                                                 winsize,
                                                 delta)

        tsfresh_data = pd.concat([df_pids, df_labels, df_label_time, features_filtered], axis=1)
        tsfresh_data = merge_ehr_features(df_ehr_time, tsfresh_data, dropna=True)
        save_data(experiment_path, "tsfresh", tsfresh_data, winsize, delta)    
    



# %% [markdown]
# # Understanding the generated data

# %%
df_result = []
win_of_interest="0h"

winsizes = ["3h", "4h", "5h", "6h"]
deltas = ["0h15t", "0h30t", "0h45t", "1h", "1h15t", "1h30t", "1h45t","2h", "3h", "4h", "5h", "6h", "7h", "8h"]

for winsize in winsizes:
    for delta in deltas:
        data = load_data(experiment_path, "raw", winsize, delta, win_of_interest)
        row = {}
        row["win_of_interest"] = win_of_interest
        row["delta"] = delta
        row["winsize"] = winsize
        row["total_number_examples"] = data.shape[0]
        df_result.append(row)
    
df_result = pd.DataFrame(df_result).pivot(index="delta", values="total_number_examples", columns="winsize")
df_result.plot()
plt.ylabel("Number of training examples")
plt.xticks(range(df_result.shape[0]), df_result.index)


# %% [markdown]
# # Baseline - Uses the last CGM value collected

# %%
def __get_result_for_last_cgm(df, feature_mapping, fold_test_idx):

    y_real = df["ground_truth"]
    y_pred = df[feature_mapping["CGM"][-1]]

    r = regression_from_sklearn(y_real, y_pred)
    r["Model"] = "Last CGM"
    r["test_fold"] = fold_test_idx
    r["featset"] = "Last CGM"

    r["winsize"] = winsize
    r["delta"] = delta
    r["win_of_interest"] = win_of_interest
    return r

from sklearn import metrics
def regression_from_sklearn(y_true, y_pred):

    results = {}
    results["R2"] = metrics.r2_score(y_true, y_pred)
    results["MAE"] = metrics.mean_absolute_error(y_true, y_pred)
    results["MSE"] = metrics.mean_squared_error(y_true, y_pred)
    results["RMSE"] = metrics.mean_squared_error(y_true, y_pred, squared=False)
    results["RMSLE"] = metrics.mean_squared_log_error(y_true, y_pred)
    #results["MAPE"] = metrics.mean_absolute_percentage_error(y_true, y_pred)
    
    return results

def run_last_cgm_baseline(experiment_path, winsize, delta, win_of_interest):

    data, feature_mapping = prepare_ml(experiment_path, "raw", winsize, delta, win_of_interest, signals)
    
    folds = data["fold"].unique()
    
    train_rows = []
    test_rows = []
    for fold_test_idx in folds:
        
        train_data, test_data = split_train_test(data, ["CGM"], feature_mapping, fold_test_idx)
        train_rows.append(__get_result_for_last_cgm(train_data, feature_mapping, fold_test_idx))
        test_rows.append(__get_result_for_last_cgm(test_data, feature_mapping, fold_test_idx))
        
    return pd.DataFrame(train_rows), pd.DataFrame(test_rows)


# %%
win_of_interest="0h"
winsizes = ["3h", "4h", "5h", "6h"]
deltas = ["0h15t", "0h30t", "0h45t", "1h", "1h15t", "1h30t", "1h45t","2h", "3h", "4h", "5h", "6h", "7h", "8h"]

rs_train = []
rs_test = []
for winsize in winsizes:
    for delta in deltas:
        r_train, r_test = run_last_cgm_baseline(experiment_path, winsize, delta, win_of_interest)
        rs_train.append(r_train)
        rs_test.append(r_test)

pd.concat(rs_train).to_csv("results/bl_train_results.csv.gz", index=False)
pd.concat(rs_test).to_csv("results/bl_test_results.csv.gz", index=False)

# %% [markdown]
# # Regression Task

# %%
data, feature_mapping = prepare_ml(experiment_path, "tsfresh", "5h", "1h", "0h", ["time", "HeartRate", "mets", "Calories", "Steps", "Distance", "CGM", "other"])

for k in feature_mapping.keys():
    print("Feature set '%s': %d features" % (k, len(feature_mapping[k])))
    
data


# %%
def run_pycaret(experiment_path, winsize, delta, win_of_interest, featset, strategy="one", models=None):

    data, feature_mapping = prepare_ml(experiment_path, "tsfresh", winsize, delta, win_of_interest, signals)

    if strategy == "one":
        folds = [data["fold"].max()]
    elif strategy == "all":
        folds = data["fold"].unique()
        
    train_results = []
    test_results = []
    best_models = []

    for fold_test_idx in folds:

        train_data, test_data = split_train_test(data, featset, feature_mapping, fold_test_idx)

        experiment = setup(data = train_data, test_data = test_data,
                       target='ground_truth', session_id=42, silent=True,
                       fold_strategy = GroupKFold(), fold_groups = 'fold',
                       ignore_features = ["pid", "fold"]
                      )

        best_model = compare_models(models)
        train_result = pull()
        
        prediction_results = predict_model(best_model)
        test_result = pull()
        
        # Saving configurations
        train_result["test_fold"] = fold_test_idx
        train_result["featset"] = "_".join(featset)
        train_result["winsize"] = winsize
        train_result["delta"] = delta
        train_result["win_of_interest"] = win_of_interest
        
        test_result["test_fold"] = fold_test_idx
        test_result["featset"] = "_".join(featset)
        test_result["winsize"] = winsize
        test_result["delta"] = delta
        test_result["win_of_interest"] = win_of_interest
        

        test_results.append(test_result)
        train_results.append(train_result)
        best_models.append(best_model)
        
    train_results = pd.concat(train_results)
    test_results = pd.concat(test_results)

    return best_models, train_results, test_results


# %%
model, train, test = run_pycaret(experiment_path, "3h", "1h", "0h", 
                                 ["time", "HeartRate", "mets", "Calories", "Steps", "Distance", "CGM", "other"],
                                 strategy="one", models=["et"])


# %%
interpret_model(model[0])

# %%
plot_model(model[0], "feature")

# %%
plot_model(model[0], "error")

# %%
interpret_model(model[0], plot = 'reason', observation = 10)

# %% [markdown]
# We run a large variaty of regression models to understand the prediction power over time

# %%
win_of_interest="0h"
featset = ['time', 'HeartRate', 'mets', 'Calories', 'Steps', 'Distance', 'other', "CGM"]
#featset = ['time', 'HeartRate', 'mets', 'Calories', 'Steps', 'Distance', 'other']
#featset = ['time', 'HeartRate', 'mets', 'Calories', 'Steps', 'Distance', 'CGM']

train_results = []
test_results = []

for winsize in winsizes:
    for delta in deltas:
        models, tmp_train, tmp_test = run_pycaret(experiment_path, winsize, delta, 
                                                  win_of_interest, featset, strategy="all", 
                                                  models=["et", "lightgbm"])
        
        train_results.append(tmp_train)
        test_results.append(tmp_test)

pd.concat(train_results).to_csv("train_results_regression.csv", index=False)
pd.concat(test_results).to_csv("test_results_regression.csv", index=False)

# %%
df_train = pd.read_csv("results/train_results_regression.csv.gz")
df_train_bl = pd.read_csv("results/bl_train_results.csv.gz")
df_res_train = pd.concat([df_train, df_train_bl])


df_test = pd.read_csv("results/test_results_regression.csv.gz")
df_train_bl = pd.read_csv("results/bl_test_results.csv.gz")
df_res_test = pd.concat([df_test, df_train_bl])

# %%
df_test[(df_test["winsize"] == "3h") & (df_test["delta"] == "1h") & (df_test["featset"] == "time_HeartRate_mets_Calories_Steps_Distance_CGM_other")]["R2"] #.mean()


# %%
def regression_plot(df, metric, winsizes=["3h", "4h", "5h", "6h"], show_error_bar=False, legend_outside=True):

    df_means = df.sort_values(["winsize", "delta", "win_of_interest"]).copy()
        
    if show_error_bar:
        # We do not need to get the mean value here.
        y_ = "%s" % (metric)
    else:
        df_means = df_means.groupby(["featset", "winsize", "delta", "win_of_interest"]).agg(["mean", "std"])
        df_means.columns = ['_'.join(k) for k in df_means.keys()]
        df_means = df_means.reset_index()       
        
        df_means = df_means.drop(columns=["test_fold_mean", "test_fold_std"])
        y_ = "%s_mean" % (metric)

    df_means = df_means[df_means["winsize"].isin(winsizes)]
    df_means["Feature Window Size"] = df_means["winsize"]
    df_means["Feature Set"] = df_means["featset"].replace({
        "time_HeartRate_mets_Calories_Steps_Distance_CGM": "FitBit+CGM-EHR",
        "time_HeartRate_mets_Calories_Steps_Distance_CGM_other": "FitBit+CGM+EHR",
        "time_HeartRate_mets_Calories_Steps_Distance_other": "Only FitBit",
    })
    
    # Plot the responses for different events and regions
    g = sns.lineplot(x="delta", y=y_,
                     hue="Feature Window Size", 
                     style="Feature Set", style_order=sorted(df_means["Feature Set"].unique()),
                     data=df_means)


    box = g.get_position()
    g.set_position([box.x0, box.y0, box.width * 0.85, box.height]) # resize position

    if legend_outside:
        # Put a legend to the right side
        g.legend(loc='center right', bbox_to_anchor=(1.40, 0.5), ncol=1)



# %%
regression_plot(df_res_train, "MAE", winsizes=["3h"], show_error_bar=True, legend_outside=False)

# %%
regression_plot(df_res_test, "MAE", winsizes=["3h", "5h"], show_error_bar=False)

# %%
# Get Best Model ("FitBit + CGM + EHR" - Winsize = 3h, predict 0h15t)

# %% [markdown]
# #### predict_model(best_model)

# %%
# plot_model(result[0][-1], "feature")
