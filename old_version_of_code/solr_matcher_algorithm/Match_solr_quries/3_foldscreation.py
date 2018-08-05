import pandas as pd
from collections import Counter
import numpy as np


def get_match_value(cats):
    cats = cats.categories
    counter = Counter(cats.get_values())
    true_match = counter.get("true_match", 0)
    true_not_match = counter.get("true_not_match", 0)
    false_match = counter.get("false_match", 0)
    false_not_match = counter.get("false_not_match", 0)
    sum_match = true_match + true_not_match + false_match + false_not_match
    match_alg = true_match + false_match
    if (match_alg != 0):
        pr = float(true_match) / (match_alg)
    else:
        pr = np.nan
    world = float(true_match + false_not_match)
    if (world != 0):
        recall = true_match / (world)
    else:
        recall = np.nan
    pr_re = pr + recall
    if (pr_re == 0):
        pr_re = np.nan
    data = {
        "precision": pr,
        "recall": recall,
        "fm": (2 * (pr * recall) / (pr_re)),
        "number_match": (true_match + false_match),
        "t_match": true_match,
        "number_not_match": (false_not_match + true_not_match),
        "t_n_match": true_not_match,
        "f_n_match": false_not_match,
        "f_match": false_match
    }

    return(data)

##################################[  Step 1   ]###################################

# 1 - it generates 10 folds
### with calculation of precision, recall, fm, number_match, t_match,
###  number_not_match, true_not_match, false_not_match and false_match
### for each query

# 2 - save the result in Data/folds

# 3 - line numbers 61,62 and 63 can be changed to each other

##################################################################################

sowiport_ms_ne=pd.read_csv("Data/saved/categories.csv", encoding='utf-8')
orgin_table=pd.read_csv("Data/saved/table_orgin_data.csv", encoding='utf-8')
sowiport_ms_ne1 = pd.merge(sowiport_ms_ne, orgin_table, on='ref_id', how='outer')



#sowiport_ms_ne2=sowiport_ms_ne1[sowiport_ms_ne1["original_table"]=="table2"]
#sowiport_ms_ne2=sowiport_ms_ne1[sowiport_ms_ne1["original_table"]=="table1"]
sowiport_ms_ne2=sowiport_ms_ne1


len_list=sowiport_ms_ne2.shape[0]/10
list_fold=[]
sowiport_ms_ne1=sowiport_ms_ne2
for item in range(0,10):
    list_fold.append(sowiport_ms_ne1.sample(n=int(len_list)))
    sowiport_ms_ne1=sowiport_ms_ne1[sowiport_ms_ne1.isin(list_fold[item])!=True].dropna()

for item in range(0, 10):
    sowiport_ms_ne = list_fold[item]
    match_df = \
    sowiport_ms_ne[(sowiport_ms_ne["categories"] == "false_match") | (sowiport_ms_ne["categories"] == "true_match")][
        ["ref_id", "keys", "categories"]]
    match_df = sowiport_ms_ne[["ref_id", "keys", "categories"]]
    match_df.columns = ["ref_id", "used_keys", "categories"]
    match_df.used_keys = match_df.used_keys.apply(lambda x: tuple(sorted(eval(x))))
    best_combinations = match_df.groupby("used_keys")[["categories"]].apply(get_match_value)

    acc_list = []
    for index, item_iter in best_combinations.iteritems():
        tdf = pd.DataFrame([item_iter])
        temp = []
        temp.append(index)
        for item_iter in tdf.loc[0]:
            temp.append(item_iter)
        acc_list.append(temp)

    best_combinations = pd.DataFrame(acc_list)
    best_combinations.columns = ["index", 'f_match', 'f_n_match', 'fm', 'number_match', 'number_not_match', 'precision',
                                 'recall', 't_match', 't_n_match']
    sortrr = best_combinations.sort_values(["precision", "recall"], ascending=False)
    sortrr = sortrr.reset_index(drop=True)
    result = sortrr[["index", "precision", "recall", "fm", "t_match", "t_n_match", "f_n_match", "f_match"]]
    result.to_csv("./Data/folds/total/fold_" + str(item) + ".csv", index=None, encoding='utf-8')