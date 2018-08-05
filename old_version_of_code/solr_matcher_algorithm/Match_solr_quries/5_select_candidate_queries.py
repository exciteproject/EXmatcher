import pandas as pd
import itertools

def all_subset(list_item):
    sub_list=[]
    for i in range(1,len(list_item)+1):
        for item in findsubsets(list_item,i):
            sub_list.append(item)
    return list(set(sub_list))

def findsubsets(S,m):
    return list(itertools.combinations(S, m))

##################################[  Step 1   ]###################################

# 1 - all data about crossvalidation load to a variable

# 2 - based on a precision threshold, some queries from the variable will be selected

# 3 - find all fields which can be exist in a query and compute whole subsets

# 4 - make a matrix that in one column contains "used keys".
### In the second column all queries which are above the threshold and only contain keys in the first column.

##################################################################################

wholematch1=pd.read_csv("Data/cross_validation_result/avg_whole.csv", encoding='utf-8')
precision_thershold=0.90
final_query=wholematch1[wholematch1["avg_precision"].isnull()!=True][wholematch1["avg_precision"]>precision_thershold].sort_values(["avg_precision","avg_tp","avg_recall"], ascending=False).reset_index(drop=True)
final_query.to_csv("Data/Dictionary_query/precision_"+str(precision_thershold).replace(".","")+"_dict_main.csv",index=False, encoding='utf-8')


final_query["index1"]=final_query["index"]
for i in range(0,len(final_query)):
    final_query.loc[i,"index1"]=str(final_query.loc[i,"index1"]).replace("(","").replace("'","").replace(")","").replace(" ","").replace(",","")
queris_picked=list(final_query["index"])
queris_picked.sort(key=len)
list_queris_picked=[]
for item in queris_picked:
    temp=[]
    temp.append(item)
    list_item=list(eval(item))
    if "" in list_item:
        list_item.remove("")
    temp.append(list(list_item))
    list_queris_picked.append(temp)
list_queris_picked=pd.DataFrame(list_queris_picked)
list_queris_picked.columns=["tuples_item","list_item"]

lqp_r=list(list_queris_picked["list_item"])
lqp_r2 = lqp_r[:]
for m in lqp_r:
    for n in lqp_r:
        if set(n).issubset(set(m)) and m != n:
            lqp_r2.remove(m)
            break
listofindex_df_q=[]
for item in lqp_r2:
    item.sort()
    tuple_it_key_shrink=tuple(item)
    #print(tuple_it_key_shrink)
    value_r_df=final_query[final_query["index1"]==str(tuple_it_key_shrink).replace("(","").replace("'","").replace(")","").replace(" ","").replace(",","")]
    listofindex_df_q.append(list(value_r_df.index)[0])
final_query=final_query.ix[listofindex_df_q]
final_query=final_query[["index","avg_precision","avg_recall","avg_tp","avg_fp"]]



l_q=[]
for item in list(wholematch1["index"]):
    temp_list=item.replace("(","").replace(")","").replace(" ", "").split(",")
    if "" in temp_list:
        temp_list.remove("")
    l_q=l_q+temp_list
filds=list(set(l_q))


filds.sort()

list_all_subsets={}
list_all_subsets1=[]
for item in all_subset(filds):
    list_temp=[]
    list_temp.append(str(item))
    list_temp.append(all_subset(list(item)))
    list_all_subsets[item]=all_subset(list(item))
    list_all_subsets1.append(list_temp)


dictionary_query=pd.DataFrame(list_all_subsets1)
dictionary_query.columns=["used_keys","combination"]
dictionary_query.used_keys = dictionary_query.used_keys.apply(lambda x: tuple(sorted(eval(x))))
final_query.columns=["used_keys","avg_precision","avg_recall","avg_tp","avg_fp"]
final_query.used_keys = final_query.used_keys.apply(lambda x: tuple(sorted(filter(None,x.replace("(","").replace(")","").replace(" ", "").split(",")))))

l_f_q=list(final_query["used_keys"])
for item in dictionary_query["used_keys"]:
    listofcombination=list(dictionary_query[dictionary_query["used_keys"]==item]["combination"])[0]
    tem_tuples=[]
    final_temp_tuples=[]
    for item1 in listofcombination:
        list_item1=list(item1)
        list_item1.sort()
        tem_tuples.append(tuple(list_item1))
    for item1 in l_f_q:
        if item1 in tem_tuples:
            final_temp_tuples.append(str(item1).replace("(","").replace(")","").replace(" ", "").split(","))
    dictionary_query.loc[dictionary_query[dictionary_query["used_keys"]==item].iloc[0].name,"combination"]=final_temp_tuples

dictionary_query.to_csv("../../matcher_algorithm/precision_"+str(precision_thershold).replace(".","")+"_dict_query.csv",index=False, encoding='utf-8')
