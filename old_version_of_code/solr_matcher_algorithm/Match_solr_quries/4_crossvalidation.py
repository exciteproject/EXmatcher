import pandas as pd
import numpy as np


list_fold=[]
for item in range(0,10):
    tem_df=pd.read_csv("Data/folds/total/fold_"+str(item)+".csv", encoding='utf-8')
    tem_df.columns=["index","precision"+str(item),"recall"+str(item),"fm"+str(item),"t_match"+str(item),"t_n_match"+str(item),"f_n_match"+str(item),"f_match"+str(item)]
    list_fold.append(tem_df)


result = pd.merge(list_fold[0],list_fold[1], on='index', how='outer')
for item in range(2,10):
    result = pd.merge(result,list_fold[item], on='index', how='outer')


result["avg_precision"] = np.nan
result["avg_recall"] = np.nan
result["avg_tp"] = np.nan
result["avg_fp"] = np.nan



for item in result["index"]:
    #temp=[]
    sum_pr=result[result["index"]==item][["precision0","precision9","precision8","precision7","precision6","precision5","precision4","precision3","precision2","precision1"]].sum(axis=1)
    count_pr=result[result["index"]==item][["precision0","precision9","precision8","precision7","precision6","precision5","precision4","precision3","precision2","precision1"]].count(axis=1)
    precision=sum_pr/count_pr
    sum_recall=result[result["index"]==item][["recall0","recall9","recall8","recall7","recall6","recall5","recall4","recall3","recall2","recall1"]].sum(axis=1)
    count_recall=result[result["index"]==item][["recall0","recall9","recall8","recall7","recall6","recall5","recall4","recall3","recall2","recall1"]].count(axis=1)
    recall=sum_recall/count_recall
    sum_tp=result[result["index"]==item][["t_match0","t_match9","t_match8","t_match7","t_match6","t_match5","t_match4","t_match3","t_match2","t_match1"]].sum(axis=1)
    count_tp=result[result["index"]==item][["t_match0","t_match9","t_match8","t_match7","t_match6","t_match5","t_match4","t_match3","t_match2","t_match1"]].count(axis=1)
    avg_tp=float(sum_tp)/count_tp
    sum_fp=result[result["index"]==item][["f_match0","f_match9","f_match8","f_match7","f_match6","f_match5","f_match4","f_match3","f_match2","f_match1"]].sum(axis=1)
    count_fp=result[result["index"]==item][["f_match0","f_match9","f_match8","f_match7","f_match6","f_match5","f_match4","f_match3","f_match2","f_match1"]].count(axis=1)
    avg_fp=float(sum_fp)/count_fp
    result.loc[result[result["index"]==item].iloc[0].name,"avg_precision"]=list(precision)[0]
    result.loc[result[result["index"]==item].iloc[0].name,"avg_recall"]=list(recall)[0]
    result.loc[result[result["index"]==item].iloc[0].name,"avg_tp"]=list(avg_tp)[0]
    result.loc[result[result["index"]==item].iloc[0].name,"avg_fp"]=list(avg_fp)[0]


sorted_result=result[["index","avg_precision","avg_recall","avg_tp","avg_fp"]].sort_values(["avg_precision","avg_tp","avg_recall"], ascending=False).reset_index(drop=True)
sorted_result.to_csv("Data/cross_validation_result/avg_whole.csv",index=None, encoding='utf-8')
