import Input_normalized_query_builder_sv as inqb
import urllib
from urllib import urlopen
from query_builder import SOLRMappingQueryBuilder
import time

solr_url = ""
finder = SOLRMappingQueryBuilder("./Mapper.json")
solr_query = {'fl': 'id', 'rows': '1', 'start': '0', 'format': 'python','api_key':''}

def query_solr(SOLR_url, solr_params, solr_query):
    solr_query['q'] = unicode(solr_params['q']).encode('utf-8')
    dict_query_url = urllib.urlencode(solr_query)
    reference_query = '%s%s' % (
        SOLR_url, dict_query_url
    )
    connection = urlopen(reference_query)
    response = connection.read()
    return eval(response)

def make_temprec(ntitles,norm_authors,listofyear,citeid):
    temprec = {}
    temprec["norm_title_str"] = ntitles
    temprec["author_name"] = norm_authors
    temprec["year_test"] = listofyear
    temprec["Cit_id"] = citeid
    return temprec

def result_checker(result,temprec):
    if (len(result['response']['docs']) != 0):
        temprec['Sowiport_id'] = result['response']['docs'][0]['id']
        result_match={'Cit_id':temprec['Cit_id'], 'Sowiport_id':temprec['Sowiport_id']}
    else:
        result_match={'Cit_id':temprec['Cit_id'], 'Sowiport_id':'not_match'}
    return result_match,temprec

def tempref_genrator(ref_id,authorlist,yearentity,title_value):
    itemref = {}
    #int(red_id) (e.g. 5)
    itemref['ID'] = str(ref_id)
    # author list(e.g. ["Berscheid"," E."," Walster"," E."])
    itemref['author'] = ','.join(authorlist)
    years=""
    #yearentity e.g [" 1974",2001]
    if len(yearentity)>0:
        for item in yearentity:
            years=years+","+str(item)
    itemref['year'] = years
    #title_value e.g. "A treatise on the family"
    itemref['title'] = str(title_value)
    return itemref

def main(input_dict):
    itemref=tempref_genrator(input_dict['ID'],input_dict['author'],input_dict['year'],input_dict['title'])
    ntitles, norm_authors,citeid, listofyear = inqb.Author_title(itemref)
    flag=0
    result_match={}
    generated_query={}
    if len(ntitles)>1:
        if len(listofyear)>0 or len(norm_authors)>0:
            flag=1
    if flag==1:
        temprec=make_temprec(ntitles,norm_authors,listofyear,citeid)
        generated_query = finder.generate_queries(temprec)
        params = {"q": generated_query['title_our_new_query']}
        result = query_solr(solr_url, params, solr_query)
        result_match,temprec = result_checker(result,temprec)
        return result_match, generated_query
    else:
        generated_query=""
        result_match={'Sowiport_id':'zero_flag'}
        return result_match, generated_query
        
#input_dict = {}
#input_dict['ID'] = 5
#input_dict['author'] = ["Berscheid"," E."," Walster"," E."]
#input_dict['year'] = [" 1974",2001]
#input_dict['title'] = "A treatise on the family" 

#resutl,param = main(input_dict)
#print resutl['Sowiport_id']