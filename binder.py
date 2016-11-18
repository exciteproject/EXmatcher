# -*- coding: UTF-8 -*-
import Input_normalized_query_builder
import urllib
from urllib import urlencode, urlopen
from query_builder import SOLRMappingQueryBuilder

#adjust solr_url
solr_url = "Set the path( for security reason not specified)/solr/solr_query.php?do=normal&"
finder = SOLRMappingQueryBuilder("./Mapper.json")

def query_solr(SOLR_url, solr_params,solr_query):
    solr_query['q']=unicode(solr_params['q']).encode('utf-8')
    dict_query_url=urllib.urlencode(solr_query)
    reference_query = '%s%s' % (
        SOLR_url, dict_query_url
    )
    connection = urlopen(reference_query)
    response = connection.read()
    return eval(response)

def main():
    list_of_refs = Input_normalized_query_builder.bibtex_parser()
    Normlized_title_author_list = []
    for item in list_of_refs:
        if len(item) > 1:
            try:
                temprec = {}
                ntitles, norm_authors = Input_normalized_query_builder.Author_title(item)
                temprec["norm_title_str"] = ntitles
                temprec["author_name"] = norm_authors
                Normlized_title_author_list.append(temprec)
            except:
                pass

    new_rec_list = []
    for item in Normlized_title_author_list:
        generated_query = finder.generate_queries(item)
        params = {"q": generated_query['title_our_new_query']}
        solr_query={'fl':'id', 'rows':'1', 'start':'0', 'format':'python'}
        result = query_solr(solr_url, params,solr_query)
        try:
            if (len(result['response']['docs']) != 0):
                item['result_id'] = result['response']['docs'][0]['id']
                new_rec_list.append(item)
        except:
            print("error")

    print (new_rec_list)
