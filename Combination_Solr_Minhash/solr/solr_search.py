# -*- coding: UTF-8 -*-
__author__ = "Behnam Ghavimi"
import urllib
from urllib import urlopen

import pandas as pd

from solr.query_builder import SOLRMappingQueryBuilder
#The Url is not mentioned comepeletly for the security reason
solr_url = "./solr_query.php?do=normal&"
finder = SOLRMappingQueryBuilder("./solr/Mapper.json")


def query_solr(SOLR_url, solr_params, solr_query):
    solr_query['q'] = unicode(solr_params['q']).encode('utf-8')
    dict_query_url = urllib.urlencode(solr_query)
    reference_query = '%s%s' % (
        SOLR_url, dict_query_url
    )
    connection = urlopen(reference_query)
    response = connection.read()
    return eval(response)


def main(Normlized_title_author_list):
    new_rec_list = []
    itemnorm= Normlized_title_author_list[0]
    generated_query = finder.generate_queries(itemnorm)
    params = {"q": generated_query['title_our_new_query']}
	#api_key is deleted
    solr_query = {'fl': 'id', 'rows': '1', 'start': '0', 'format': 'python', 'api_key': ''}
    result = query_solr(solr_url, params, solr_query)
    try:
        if (len(result['response']['docs']) != 0):
            id_sowiport = result['response']['docs'][0]['id']
            Flag_match='true'
        else:
            id_sowiport=''
            Flag_match = 'false'
    except:
        id_sowiport='error'
        Flag_match = 'false'

    return Flag_match,id_sowiport
