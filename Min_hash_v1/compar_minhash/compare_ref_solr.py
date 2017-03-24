__author__ = "Behnam Ghavimi,ottowg"
import pandas as pd
from query_builder import SOLRMappingQueryBuilder
from urllib import urlencode, urlopen
from time import time
from sklearn.metrics import jaccard_similarity_score
from scipy import spatial

list_solr_h = pd.read_csv("./data_minhash/list_title_solr_hashes.csv")
list_ref_h = pd.read_csv("./data_minhash/list_ref_hashes_with_correction.csv")

next_prime = 4294967311

#for security reason, the url is not mentioned completely
solr_url = "url_should_be_mention_here/solr_query.php?do=normal&"
finder = SOLRMappingQueryBuilder("./Mapper.json")


def compare_min_hash(one, two):
    """
    Comparing two hashes
    @return the approximated jaccard distance generated from two hashes
    """
    nr_same = sum([1 for nr in range(len(one)) if
                  one[nr] == two[nr]])

    return float(float(nr_same)/len(one))

def query_solr(SOLR_url, solr_params,solr_query):
    solr_query['q']=unicode(solr_params['q']).encode('utf-8')
    dict_query_url=urlencode(solr_query)
    reference_query = '%s%s' % (
        SOLR_url, dict_query_url
    )
    connection = urlopen(reference_query)
    response = connection.read()
    return eval(response)

def result_solr_id(item):
    generated_query = finder.generate_queries(item)
    params = {"q": generated_query['our_new_query']}
    solr_query = {'fl': 'id', 'rows': '10000000', 'start': '0', 'format': 'python'}
    result = query_solr(solr_url, params, solr_query)
    result = result['response']['docs']
    return result

list_solr_h=list_solr_h.set_index('id')

t0 = time()
reult_final_list = []
for idx, item_ref in enumerate(list_ref_h['author_name'][:100]):
    print(idx)
    query_item = {"author_name": eval(item_ref)}
    list_of_id_in_solr = result_solr_id(query_item)
    ref_info_list = list_ref_h.iloc[[idx]].get_values()
    ref_hash_list = eval(ref_info_list[0][2])
    maxi = {'id': '', 'value': 0}
    for item in list_of_id_in_solr:
        id_solr_index = item['id']
        try:
            vector_solr_item = list_solr_h.loc[id_solr_index].get_values()
        except:
            # vector_solr_item=list_solr_h.loc['wzb-bib-135668'].get_values()
            # print('error'+id_solr_index)
            pass

        # compare_value=compare_min_hash(ref_hash_list,vector_solr_item)
        # compare_value=jaccard_similarity_score(ref_hash_list,vector_solr_item)
        compare_value = 1 - spatial.distance.cosine(ref_hash_list, vector_solr_item)
        if compare_value > maxi['value']:
            maxi['id'] = id_solr_index
            maxi['value'] = compare_value
    if maxi['value'] > 0.785:
        temp_rec = {}
        temp_rec['filename'] = list_ref_h['filename'][idx]
        temp_rec['norm_title_str'] = list_ref_h['norm_title_str'][idx]
        temp_rec['jaccard'] = maxi
        print(temp_rec)
        reult_final_list.append(temp_rec)
print(len(reult_final_list))

t1 = time()
print(t1 - t0)

