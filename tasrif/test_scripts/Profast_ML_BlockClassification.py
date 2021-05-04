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
# ## Scenario 2
# ## Machine Learning to predict the CGM of a given future data block

# %%
# %load_ext autoreload
# %autoreload 2

from sklearn.model_selection import LeaveOneGroupOut, KFold, GroupKFold

from pycaret.classification import *

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
experiment_path = "../../data/profast2020/scenario2/"

# %%
signals = ["time", "patientID", "HeartRate", "mets", "Calories", "Steps", "Distance", "CGM"]

winsizes = ["3h", "4h", "5h"]
deltas = ["0h"] 
wins_of_interest = ["1h", "2h", "3h", "4h", "5h", "6h", "7h", "8h"]
label_cols = ["hyper", "hypo", "shypo"]

params = []
for winsize in winsizes:
    for delta in deltas:
        for win_of_interest in wins_of_interest:
            params.append([winsize, delta, win_of_interest])
        
print("Processing %d param combinations." % (len(params)))

time_features = [k for k in df.keys() if k.startswith("time")]
df_ehr_time = pd.merge(df[["patientID", *time_features]], df_emr)

for param in tqdm(params):
    
    winsize, delta, win_of_interest = param
    
    if data_exists(experiment_path, "tsfresh", winsize, delta, win_of_interest) and data_exists(experiment_path, "raw", winsize, delta, win_of_interest):
        print("%s: %s, %s, %s was already processed" % (experiment_path, winsize, delta, win_of_interest))
        continue

    df_timeseries, df_labels, df_label_time, df_pids = generate_timeseries_df_from_block(df, signals, winsize, delta, win_of_interest, label_cols=label_cols)
    
    if not data_exists(experiment_path, "raw", winsize, delta):
        raw_features = calculate_raw_features(df_timeseries, ["CGM", "HeartRate", "mets", "Calories", "Steps", "Distance"], "seq_id")
        raw_data = pd.concat([df_pids, df_labels, df_label_time, raw_features], axis=1)
        raw_data = merge_ehr_features(df_ehr_time, raw_data, dropna=True)
        save_data(experiment_path, "raw", raw_data, winsize, delta, win_of_interest)

    if not data_exists(experiment_path, "tsfresh", winsize, delta):
        features_filtered = extract_features_from_df(experiment_path, df_timeseries, df_labels["hyper"] > 0, winsize, delta, win_of_interest)
    
        tsfresh_data = pd.concat([df_pids, df_labels, df_label_time, features_filtered], axis=1)
        tsfresh_data = merge_ehr_features(df_ehr_time, tsfresh_data, dropna=True)
        save_data(experiment_path, "tsfresh", tsfresh_data, winsize, delta, win_of_interest)    


# %% [markdown]
# # Understanding the generated data

# %%
def describe_gt_balance(data, gt_strategy, verbose=False, return_label=True):
    gt = define_ground_truth(data, gt_strategy)
    if verbose:
        for v in gt.unique():
            print("Number of values of %s: %d (%.2f%%)" % (v, (gt==v).sum(), 100.*(gt==v).sum()/gt.shape[0]))        
    return (gt==return_label).sum(), 100.*(gt==return_label).sum()/gt.shape[0]

def distribution_gt(data, gt_strategy):
    gt = define_ground_truth(data, gt_strategy)
    c = {}
    for v in gt.unique():
        c[v] = (gt==v).sum()
    return c
    
    

# %%
df_result = []
win_of_interest="0h"

winsizes = ["3h", "4h", "5h"]
wins_of_interest = ["1h", "2h", "3h", "4h", "5h", "6h", "7h", "8h"]
gt_strategies = ["hyper", "hypo"]
delta = "0h"

for winsize in winsizes:
    for win_of_interest in wins_of_interest:
        for gt_strategy in gt_strategies:
            data = load_data(experiment_path, "raw", winsize, delta, win_of_interest)
            
            n, p = describe_gt_balance(data, gt_strategy)

            row = {}
            row["win_of_interest"] = win_of_interest
            row["delta"] = delta
            row["winsize"] = winsize
            row["total_number_examples"] = data.shape[0]
            row["gt_strategy"] = gt_strategy
            row["n"] = n
            row["p"] = p
            df_result.append(row)
    
df_result = pd.DataFrame(df_result)

# %%
n_examples = df_result[["win_of_interest", "total_number_examples", "winsize"]].drop_duplicates().pivot(index="win_of_interest", values="total_number_examples", columns="winsize")
n_examples.plot()
plt.ylabel("Number of training examples")
plt.xticks(range(n_examples.shape[0]), n_examples.index)

# %%
g = sns.lineplot(x="win_of_interest", y="p",
                 hue="gt_strategy", style="winsize",
                 data=df_result)
plt.ylabel("Number of events (%)")


# %% [markdown]
# # ML Task

# %%
def run_pycaret(experiment_path, winsize, delta, win_of_interest, 
                featset, strategy="one", 
                gt_strategy="hyper", models=None, sort_metric = "MCC"):

    data, feature_mapping = prepare_ml(experiment_path, "tsfresh", winsize, delta, win_of_interest, signals)
    
    if strategy == "one":
        folds = [data["fold"].max()]
    elif strategy == "all":
        folds = data["fold"].unique()
        
       
    train_results = []
    test_results = []
    best_models = []

    for fold_test_idx in folds:

        data["ground_truth"] = define_ground_truth(data, gt_strategy)

        train_data, test_data = split_train_test(data, featset, feature_mapping, fold_test_idx)
               
        ignore_features = []
        for col in ["pid", "fold", "hyper", "hypo", "shypo"]:
            if col in train_data:
                ignore_features.append(col)
        
        experiment = setup(data = train_data, test_data = test_data,
                       target='ground_truth', session_id=42, silent=True,
                       fold_strategy = GroupKFold(), fold_groups = 'fold',
                       ignore_features = ignore_features
                      )

        best_model = compare_models(models, sort=sort_metric)
        train_result = pull()
        
        prediction_results = predict_model(best_model)
        test_result = pull()
        
        # Saving configurations
        train_result["test_fold"] = fold_test_idx
        train_result["featset"] = "_".join(featset)
        train_result["winsize"] = winsize
        train_result["delta"] = delta
        train_result["win_of_interest"] = win_of_interest
        train_result["gt_strategy"] = gt_strategy
        
        test_result["test_fold"] = fold_test_idx
        test_result["featset"] = "_".join(featset)
        test_result["winsize"] = winsize
        test_result["delta"] = delta
        test_result["win_of_interest"] = win_of_interest
        test_result["gt_strategy"] = gt_strategy
        

        test_results.append(test_result)
        train_results.append(train_result)
        best_models.append(best_model)
        
    train_results = pd.concat(train_results)
    test_results = pd.concat(test_results)

    return best_models, train_results, test_results


# %%
def run_pycaret_single_CGM(experiment_path, winsize, delta, win_of_interest, 
                           featset, strategy="one", 
                           gt_strategy="hyper", models=None, sort_metric = "MCC"):

    data, feature_mapping = prepare_ml(experiment_path, "tsfresh", winsize, delta, win_of_interest, signals)
    raw_data, raw_fm = prepare_ml(experiment_path, "raw", winsize, delta, win_of_interest, signals + ["CGM"])
    feature_mapping["CGM"] = ["CGM"]
    
    if strategy == "one":
        folds = [data["fold"].max()]
    elif strategy == "all":
        folds = data["fold"].unique()
        
       
    train_results = []
    test_results = []
    best_models = []

    for fold_test_idx in folds:

        data["ground_truth"] = define_ground_truth(data, gt_strategy)
        data["CGM"] = raw_data[raw_fm["CGM"][4]]
        
        #return data, feature_mapping
        train_data, test_data = split_train_test(data, featset + ["CGM"], feature_mapping, fold_test_idx)
               
        if "CGM" not in train_data:
            print("CGM was not correctly added! ERROR")
            return
            
        ignore_features = []
        for col in ["pid", "fold", "hyper", "hypo", "shypo"]:
            if col in train_data:
                ignore_features.append(col)
        
        experiment = setup(data = train_data, test_data = test_data,
                       target='ground_truth', session_id=42, silent=True,
                       fold_strategy = GroupKFold(), fold_groups = 'fold',
                       ignore_features = ignore_features
                      )

        best_model = compare_models(models, sort=sort_metric)
        train_result = pull()
        
        prediction_results = predict_model(best_model)
        test_result = pull()
        
        # Saving configurations
        train_result["test_fold"] = fold_test_idx
        train_result["featset"] = "_".join(featset)
        train_result["winsize"] = winsize
        train_result["delta"] = delta
        train_result["win_of_interest"] = win_of_interest
        train_result["gt_strategy"] = gt_strategy
        
        test_result["test_fold"] = fold_test_idx
        test_result["featset"] = "_".join(featset)
        test_result["winsize"] = winsize
        test_result["delta"] = delta
        test_result["win_of_interest"] = win_of_interest
        test_result["gt_strategy"] = gt_strategy
        

        test_results.append(test_result)
        train_results.append(train_result)
        best_models.append(best_model)
        
    train_results = pd.concat(train_results)
    test_results = pd.concat(test_results)

    return best_models, train_results, test_results

# %% [markdown]
# Run many experiments...this next cell will take a long time to finish

# %%
winsize = "3h"
delta = "0h"
win_of_interest = "3h"
featset = ["time", "HeartRate", "mets", "Calories", "Steps", "Distance", "other"]

models, train, test = run_pycaret_single_CGM(experiment_path, winsize, delta, win_of_interest, 
                                 featset, strategy="one",
                                 gt_strategy = "hyper", 
                                 #models=["gbc", "rf", "lightgbm", "et"]
                                 models=["xgboost"]
                                 )

# %%
delta = "0h"
featset = ['time', 'HeartRate', 'mets', 'Calories', 'Steps', 'Distance', 'CGM', 'other']
#featset = ['time', 'HeartRate', 'mets', 'Calories', 'Steps', 'Distance', 'other']
#featset = ['time', 'HeartRate', 'mets', 'Calories', 'Steps', 'Distance', 'CGM']

for winsize in winsizes:
    train_results = []
    test_results = []

    for win_of_interest in wins_of_interest:
        for gt_strategy in ["hypo", "hyper", "both"]:
            
            models, tmp_train, tmp_test = run_pycaret(experiment_path, winsize, delta, win_of_interest, featset, strategy="all",
                                                      gt_strategy = "hyper", sort_metric = "MCC",
                                                      models=["gbc", "rf", "lightgbm", "et"])

            train_results.append(tmp_train)
            test_results.append(tmp_test)

    pd.concat(train_results).to_csv("train_results_classification_win%s.csv" % (winsize))
    pd.concat(test_results).to_csv("test_results_classification_win%s.csv" % (winsize))


# %%
winsize = "3h"
delta = "0h"
win_of_interest = "3h"
featset = ["time", "HeartRate", "mets", "Calories", "Steps", "Distance", "CGM", "other"]

models, train, test = run_pycaret(experiment_path, winsize, delta, win_of_interest, 
                                 featset, strategy="one",
                                 gt_strategy = "hyper", 
                                 #models=["gbc", "rf", "lightgbm", "et"]
                                 models=["xgboost"]
                                 )

# %%
plot_model(models[0], 'feature')

# %%
plot_model(models[0], 'confusion_matrix')

# %%
interpret_model(models[0])

# %% [markdown]
# # Result Analysis
#
# Get all the results calculated above and plot the numbers

# %%
df_train = pd.read_csv("results/train_results_block.csv.gz")
df_test = pd.read_csv("results/test_results_block.csv.gz")


# %%
def classification_plot(df, metric, gt_strategy="hyper",
                        winsizes=["3h", "4h", "5h", "6h"], show_error_bar=False, legend_outside=True):

    df_means = df.sort_values(["winsize", "delta", "win_of_interest"]).copy()
    df_means = df_means[df_means["gt_strategy"] == gt_strategy]
    
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
    g = sns.lineplot(x="win_of_interest", y=y_,
                     hue="Feature Window Size", 
                     style="Feature Set", style_order=sorted(df_means["Feature Set"].unique()),
                     data=df_means)

    box = g.get_position()
    g.set_position([box.x0, box.y0, box.width * 0.85, box.height]) # resize position

    if legend_outside:
        # Put a legend to the right side
        g.legend(loc='center right', bbox_to_anchor=(1.40, 0.5), ncol=1)

    g.set(title="Label Strategy = %s" % (gt_strategy))


# %%
classification_plot(df_test, "MCC", gt_strategy="hypo",
                    winsizes=["3h", "4h", "5h"], show_error_bar=False, legend_outside=False)
