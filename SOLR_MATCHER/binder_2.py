# -*- coding: UTF-8 -*-
__author__ = "Behnam Ghavimi"

import Input_normalized_query_builder_sv as inqb
import urllib
from urllib import urlopen
from query_builder import SOLRMappingQueryBuilder
import pandas as pd
import time

# adjust solr_url
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

def main(bibtex_parser_source='./bibtex',result_save_cv_dir='./'):
    t0 = time.time()
    listoffiles = inqb.listoffile_retriver(bibtex_parser_source)
    for itemfilename in listoffiles:
        list_of_refs=inqb.listrefinafile(itemfilename,bibtex_parser_source)

        Normlized_title_author_list = []
        for itemref in list_of_refs:
            if len(itemref[0]) > 1:
                try:
                    temprec = {}
                    ntitles, norm_authors,citeid, listofyear = inqb.Author_title(itemref[0])
                    temprec["norm_title_str"] = ntitles
                    temprec["author_name"] = norm_authors
                    temprec["year_test"] = listofyear
                    #temprec["filename"] = itemref[1]
                    temprec["Cit_id"] = citeid
                    Normlized_title_author_list.append(temprec)
                except:
                    pass


        new_rec_list = []

        for itemnorm in Normlized_title_author_list:
            generated_query = finder.generate_queries(itemnorm)
            params = {"q": generated_query['title_our_new_query']}
			#api_key is deleted
            solr_query = {'fl': 'id', 'rows': '1', 'start': '0', 'format': 'python','api_key':''}
            result = query_solr(solr_url, params, solr_query)
            try:
                if (len(result['response']['docs']) != 0):
                    itemnorm['Sowiport_id'] = result['response']['docs'][0]['id']
                    new_rec_list.append({'Cit_id':itemnorm['Cit_id'], 'Sowiport_id':itemnorm['Sowiport_id']})
            except:
                print("error")

        t1 = time.time()

        total = t1 - t0
        print(total)

       # if len(new_rec_list)!=0:
            #savedfilename=result_save_cv_dir+ itemfilename+'.csv'
        #else:
            #savedfilename = result_save_cv_dir + 'empty#data_fuzzy#' + itemfilename + '.csv'
        savedfilename = result_save_cv_dir + 'result' + '.csv'
        df = pd.DataFrame(new_rec_list)
        df.to_csv(savedfilename)
        writer = pd.ExcelWriter(savedfilename.replace('csv','xlsx'), engine='xlsxwriter')
        df.to_excel(writer, sheet_name='Sheet1')
        writer.save()
main("./Gold_Standard_Wp2/cermine","./Cermine_result/")