#python 2.7
from query_builder import SOLRMappingQueryBuilder
import pandas as pd
import numpy as np
import urllib
from urllib import urlopen


solr_url = ""
finder = SOLRMappingQueryBuilder("./Mapper.json")
solr_query = {'fl': 'id', 'rows': '1', 'start': '0', 'format': 'python','api_key':''}


##################################[  Step 1   ]###################################

# 1 - read all possible dictionaries for each record

# 2 - generate query for each dictionary

# 3 - save queries in a csv file "sowiport_results_queries_utf8"

##################################################################################

def query_solr(SOLR_url, solr_params, solr_query):
    solr_query['q'] = unicode(solr_params['q']).encode('utf-8')
    dict_query_url = urllib.urlencode(solr_query)
    reference_query = '%s%s' % (
        SOLR_url, dict_query_url
    )
    #print(reference_query)
    connection = urlopen(reference_query)
    response = connection.read()
    return eval(response),reference_query

def result_checker(result):
    if (len(result['response']['docs']) != 0):
        result_match = result['response']['docs'][0]['id']
    else:
        result_match='not_match'
    return result_match



def result_solr(temprec):
    generated_query = finder.generate_queries(temprec)
    params = {"q": generated_query['title_our_new_query']}
    result,url_query = query_solr(solr_url, params, solr_query)
    return result_checker(result),url_query


df=pd.read_csv("Data/saved/sdq.csv", encoding='utf-8')
df["keys"] = np.nan
df["solr_result"] = np.nan
df["solr_query"] = np.nan

for index, row in df.iterrows():
    try:
        dict_temp=eval(row["subset_data_query"])
        if "journal" in dict_temp.keys():
            #dict_temp["journal"]=dict_temp["journal"].decode('utf-8')
            dict_temp["journal"] = unicode(dict_temp["journal"])
        df.ix[index, 'keys'] = str(dict_temp.keys())
        result_id,url_query=result_solr(dict_temp)
        df.ix[index, 'solr_result'] = result_id
        df.ix[index, 'solr_query'] = url_query
        #print(index)
        #print("***")
    except Exception as e:
        #print(e)
        df.ix[index, 'solr_result'] = "error"
        df.ix[index, 'solr_query'] = ""
        print("error"+str(index))
        #print("***")


df.to_csv("Data/saved/sowiport_results_queries_utf8.csv",index=None, encoding='utf-8')






##################################[  Step 2   ]###################################

# 4 - save categorised table for each records ["true_not_match", "true_match", "false_not_match" and "false_match"]

##################################################################################




sowiport_queries=pd.read_csv("Data/saved/sowiport_results_queries_utf8.csv")
data_ref_norm=pd.read_csv("Data/saved/data_ref_norm.csv")
ref_manual=data_ref_norm[["ref_id","sowiport_id1"]]
ref_manual.columns=["ref_id","sowiport_manual"]
ref_manual=pd.DataFrame(ref_manual)

sowiport_manual_solr=pd.merge(sowiport_queries, ref_manual, on='ref_id')
error_df=sowiport_manual_solr[sowiport_manual_solr["solr_result"]=="error"]
sowiport_ms_ne=sowiport_manual_solr[sowiport_manual_solr.isin(error_df)!=True].dropna()


sowiport_ms_ne["categories"] = np.nan
for index, row in sowiport_ms_ne.iterrows():
    if (row["solr_result"]=="not_match" and len(eval(row["sowiport_manual"]))==0):
        flag="true_not_match"
    elif (row["solr_result"] in eval(row["sowiport_manual"])):
        flag="true_match"
    elif (row["solr_result"]=="not_match" and len(eval(row["sowiport_manual"]))>0):
        flag="false_not_match"
    else:
        flag="false_match"
    sowiport_ms_ne.ix[index, 'categories']=flag

sowiport_ms_ne.to_csv("Data/saved/categories.csv",index=None, encoding='utf-8')