import pandas as pd
import itertools


def all_subset(list_item):
    sub_list = []
    for i in range(1, len(list_item) + 1):
        for item in findsubsets(list_item, i):
            sub_list.append(item)
    return list(set(sub_list))


def findsubsets(S, m):
    return list(itertools.combinations(S, m))


def main_list_prescion_keycombi(precision_thershold = -1):
    wholematch1 = pd.read_csv("./matcher_query/data/avg_whole.csv", encoding='utf-8')
    final_query = wholematch1[wholematch1["avg_precision"].isnull() != True][
        wholematch1["avg_precision"] > precision_thershold].sort_values(["avg_precision", "avg_tp", "avg_recall"],
                                                                        ascending=False).reset_index(drop=True)
    final_query.to_csv(
        "./matcher_query/data/precision_" + str(precision_thershold).replace(".", "") + "_dict_main.csv", index=False,
        encoding='utf-8')

    l_q = []
    for item in list(wholematch1["index"]):
        temp_list = item.replace("(", "").replace(")", "").replace(" ", "").split(",")
        if "" in temp_list:
            temp_list.remove("")
        l_q = l_q + temp_list
    filds = list(set(l_q))

    filds.sort()

    list_all_subsets = {}
    list_all_subsets1 = []
    for item in all_subset(filds):
        list_temp = []
        list_temp.append(str(item))
        list_temp.append(all_subset(list(item)))
        list_all_subsets[item] = all_subset(list(item))
        list_all_subsets1.append(list_temp)

    dictionary_query = pd.DataFrame(list_all_subsets1)
    dictionary_query.columns = ["used_keys", "combination"]
    dictionary_query.used_keys = dictionary_query.used_keys.apply(lambda x: tuple(sorted(eval(x))))
    final_query.columns = ["used_keys", "avg_precision", "avg_recall", "avg_tp", "avg_fp"]
    final_query.used_keys = final_query.used_keys.apply(
        lambda x: tuple(sorted(filter(None, x.replace("(", "").replace(")", "").replace(" ", "").split(",")))))

    l_f_q = list(final_query["used_keys"])
    for item in dictionary_query["used_keys"]:
        listofcombination = list(dictionary_query[dictionary_query["used_keys"] == item]["combination"])[0]
        tem_tuples = []
        final_temp_tuples = []
        for item1 in listofcombination:
            list_item1 = list(item1)
            list_item1.sort()
            tem_tuples.append(tuple(list_item1))
        for item1 in l_f_q:
            if item1 in tem_tuples:
                final_temp_tuples.append(str(item1).replace("(", "").replace(")", "").replace(" ", "").split(","))
        dictionary_query.loc[
            dictionary_query[dictionary_query["used_keys"] == item].iloc[0].name, "combination"] = final_temp_tuples

    if precision_thershold > 0:
        dictionary_query.to_csv(
            "./matcher_query/data/precision_" + str(precision_thershold).replace(".", "") + "_dict_query.csv", index=False,
            encoding='utf-8')
    else:
        dictionary_query.to_csv("./matcher_query/data/precision_main_dict_query.csv", index=False, encoding='utf-8')
