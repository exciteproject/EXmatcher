import json
import pandas as pd
from Features_extraction_support import *
from aux_function import *
from matcher_query.main_matcher import result_for_match
from matcher_query.normalizer_function import *
from tqdm import tqdm

configfile = json.load(open('./configfile.json'))

def clean(listi):
    cleaner = [l.strip() if not l.endswith("'") else l.strip()[2:-1] for l in listi]
    nls = []
    for item in cleaner:
        item1 = item.strip().split(",")
        for item2 in item1:
            nls.append(str(item2.replace("'", "").replace(" ", "")))
    return nls


def zeroone(x):
    if x == False:
        return 0
    else:
        return 1


def dtg(refsegdictitem, resultitem, reftextstring):
    authorfeatures = authores_features(refsegdictitem, resultitem)  # checked
    titlefeatures = title_features(refsegdictitem, resultitem)
    publishyearfeatures = publishyear_features(refsegdictitem, resultitem)
    pagefeatures = page_features(refsegdictitem, resultitem)
    numberfeatures = number_features(refsegdictitem, resultitem)
    journalfeatures = journal_features(refsegdictitem, resultitem)
    ls_vvd=sowiportfeatureinrefstring(refsegdictitem,resultitem, reftextstring)
    allfeatures = authorfeatures + titlefeatures + publishyearfeatures + pagefeatures + numberfeatures + journalfeatures+ls_vvd

    return allfeatures


def featuers_labels_generator_for_training():
    input_data = pd.read_csv(configfile['goldstandarddata'])
    input_data = input_data[input_data["ref_seg_dic"] != "not well-formed (invalid token): line 1, column 974"]
    input_data["ref_seg_dic"] = input_data["ref_seg_dic"].apply(json.loads)
    input_data["Maped_dict"] = input_data["ref_seg_dic"].apply(dict_generator)
    tqdm.pandas()
    input_data["result"] = input_data.progress_apply(lambda x : result_for_match(x["Maped_dict"],x["ref_text_x"]), axis=1)
    ls_expand = []
    for index, item in input_data.iterrows():
        for item1 in item["result"]:
            match_id = item1["match_id"]
            if match_id != "not_match":
                for item2 in match_id:
                    tempdict = {}
                    tempdict["ref_id"] = item["new_id"]
                    tempdict["sowi_id"] = item["sowi_id"]
                    tempdict["ref_text"] = item["ref_text_x"]
                    tempdict["ref_seg_dic"] = item["ref_seg_dic"]
                    index_in_list = list(item2.keys())[0]
                    tempdict["index_in_list"] = index_in_list
                    tempdict["result"] = item2[index_in_list]
                    tempdict["result_sowi_id"] = item2[index_in_list]["id"]
                    ls_expand.append(tempdict)

    expanded_df = pd.DataFrame(ls_expand)
    expanded_df.sort_values('index_in_list', inplace=True)
    expanded_df.drop_duplicates(subset=['ref_id', 'result_sowi_id'], keep='first', inplace=True)
    expanded_df.sort_values('ref_id', inplace=True)
    expanded_df['sowi_id'] = expanded_df[['sowi_id']].fillna(value="[]")
    expanded_df['sowi_id'] = expanded_df['sowi_id'].apply(eval)
    ls_chech_result = []
    for index, item in expanded_df.iterrows():
        ls_chech_result.append({"i": index, "v": item["result_sowi_id"] in item["sowi_id"]})
    df_result_check = pd.DataFrame(ls_chech_result)
    df_result_check.set_index("i", inplace=True, drop=True)
    expanded_df['result_check'] = df_result_check
    expanded_df.reset_index(inplace=True, drop=True)
    expanded_df["features"] = expanded_df.progress_apply(lambda row: dtg(row['ref_seg_dic'], row['result'], row["ref_text"]), axis=1)
    expanded_df["result_check1"] = expanded_df["result_check"].apply(zeroone)
    expanded_df['target'] = expanded_df[['result_sowi_id', 'sowi_id']].apply(lambda x: x[0] in clean(x[1]), axis=1)
    expanded_df[["ref_id", "features", "target"]].to_csv("data_generated/features_label.csv")


featuers_labels_generator_for_training()
