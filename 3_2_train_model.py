import pickle
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC
import json
import sys


def train_model(flag="svm"):
    expanded_df = pd.read_csv("data_generated/features_label.csv")
    expanded_df = expanded_df[["ref_id", "features", "target"]]

    if flag == "svm":
        clf = SVC(probability=True)
    elif flag == "rfc":
        clf = RandomForestClassifier(class_weight="balanced", min_samples_split=2, n_estimators=10, random_state=0)

    clf.fit(list(expanded_df["features"].apply(lambda x: json.loads(x))), list(expanded_df["target"]))

    filename = 'finalized_model' + flag + '.sav'
    pickle.dump(clf, open("data_generated/" + filename, 'wb'))

    
train_model(sys.argv[1])
