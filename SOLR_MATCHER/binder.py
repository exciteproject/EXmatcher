# -*- coding: UTF-8 -*-
__author__ = "Behnam Ghavimi"

import Input_normalized_query_builder_sv as inqb
import urllib
from urllib import urlopen
from query_builder import SOLRMappingQueryBuilder
import time

#for security reason the url is not compelete
solr_url = "url_is_deleted/solr_query.php?do=normal&"
finder = SOLRMappingQueryBuilder("./Mapper.json")


def query_solr(SOLR_url, solr_params, solr_query):
    solr_query['q'] = unicode(solr_params['q']).encode('utf-8')
    dict_query_url = urllib.urlencode(solr_query)
    reference_query = '%s%s' % (
        SOLR_url, dict_query_url
    )
    connection = urlopen(reference_query)
    response = connection.read()
    return eval(response)

def main(itemref):
        t0 = time.time()

        temprec = {}

        if len(itemref) > 1:
                try:
                    temprec = {}
                    ntitles, norm_authors,citeid, listofyear = inqb.Author_title(itemref)
                    temprec["norm_title_str"] = ntitles
                    temprec["author_name"] = norm_authors
                    temprec["year_test"] = listofyear
                    temprec["Cit_id"] = citeid
                except:
                    pass



        generated_query = finder.generate_queries(temprec)
        params = {"q": generated_query['title_our_new_query']}
        solr_query = {'fl': 'id', 'rows': '1', 'start': '0', 'format': 'python', 'api_key': ''}

        result = query_solr(solr_url, params, solr_query)
        try:
            if (len(result['response']['docs']) != 0):
                temprec['Sowiport_id'] = result['response']['docs'][0]['id']
                result_match={'Cit_id':temprec['Cit_id'], 'Sowiport_id':temprec['Sowiport_id']}
        except:
            print("error")

        t1 = time.time()

        total = t1 - t0
        print(total)

        return (result_match)

#=====example input=============
#input_dict={}
#input_dict['ID']='ID102'
#input_dict['title']='Outline of a Practical Theory of Football Violence'
#input_dict['year']='1995'
#input_dict['ENTRYTYPE']="article"
#input_dict['author']="King, A."
#=============================
input_dict={}
print(main(input_dict))