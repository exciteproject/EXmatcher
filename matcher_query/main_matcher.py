# -*- coding: UTF-8 -*-
# import sys
# reload(sys)
# sys.setdefaultencoding('utf8')
import pandas as pd
from matcher_query.query_builder import SOLRMappingQueryBuilder
import urllib
from urllib.request import urlopen
from matcher_query.normalizer_function import *
from matcher_query.listofcombination_precision import *
import string
import json


configfile = json.load(open('./configfile.json'))

dict_tags = {"journal": "journal", "doi": "doi", "number": "norm_number", "year": "norm_year", "author": "norm_author",
             "title": "norm_title", "pages": "norm_pages"}
r_dict_tags = {"journal": "journal", "doi": "doi", "norm_number": "number", "norm_year": "year",
               "norm_author": "author", "norm_title": "title", "norm_pages": "pages", "title_full":"title_full"}

solr_url = "http://sowiportbeta.gesis.org/devwork/service/solr/solr_query.php?do=normal&"
finder = SOLRMappingQueryBuilder("./matcher_query/Mapper.json")

    
solr_query = {'fl': 'id,title_full,title_sub,facet_person_str_mv,person_author_normalized_str_mv,norm_title_str,norm_title_full_str,norm_publishDate_str,norm_pagerange_str,recorddoi_str_mv,norm_volume_str,norm_issue_str,journal_title_txt_mv,journal_short_txt_mv,zsabk_str',
    'rows': '5', 'start': '0', 'format': 'python','api_key':configfile["api_key"]}


def preprocess_dict_query(dict_query):
    list_combintation = []
    for item in dict_query.iterrows():
        tem_rec = []
        used_keys = str(item[1]["used_keys"]).replace("'", "").replace("\"", "").replace("'", "").replace("(", "").replace(")",
                                                                                                          "").replace(
            " ", "").split(",")
        if "" in used_keys:
            used_keys.remove("")
        if "''" in used_keys:
            used_keys.remove("''")
        used_keys.sort()
        used_keys = (str(used_keys).replace("\'", "").replace("[", "").replace("]", "").replace(" ", ""))
        tem_rec.append(used_keys)
        combination_keys = eval(str(item[1]["combination"]).replace("\\", "").replace("u'", "'").replace(" ", "").replace("\"\"\'", "'"))
        if "''" in combination_keys:
            combination_keys.remove("''")
        if "" in combination_keys:
            combination_keys.remove("")
        tem_rec.append(combination_keys)
        list_combintation.append(tem_rec)
    list_combintation = pd.DataFrame(list_combintation)
    return (list_combintation)


thereshold_input_for_presicsion = 0.6
main_list_prescion_keycombi(thereshold_input_for_presicsion)
dict_query = pd.read_csv(
    "./matcher_query/data/precision_" + str(
        thereshold_input_for_presicsion).replace(".", "") + "_dict_query.csv")
list_combintation = preprocess_dict_query(dict_query)
list_combintation.columns = ["field_key", "combination_keys"]
list_combintation.set_index("field_key", inplace=True)


def dic_query_qen(item_ref_combi, dict_ref_parsed):
    temp = {}
    for item in item_ref_combi:
        item=item.replace("'","")
        if item!="":
            temp[item] = dict_ref_parsed[dict_tags[item]]
    return temp


def result_checker1(result):
    if (len(result['response']['docs']) != 0):
        result_match = []
        for index, item in enumerate(result['response']['docs']):
            result_match.append({index: item})
    else:
        result_match = 'not_match'
    return result_match


def query_solr(SOLR_url, solr_params, solr_query):
    solr_query['q'] = solr_params['q']
    dict_query_url = urllib.parse.urlencode(solr_query)
    reference_query = '%s%s' % (
        SOLR_url, dict_query_url
    )
    connection = urlopen(reference_query)
    response = connection.read()
    return eval(response), reference_query

    
def get_bi(stringrr):
    str_list = stringrr.split()
    bigrams = list(zip(str_list[:-1], str_list[1:]))
    return bigrams    
def preprocessed_data(dict_bib_parsed):
    dict_bib_keys = dict_bib_parsed.keys()
    temprec = {}
    for item in dict_bib_keys:
        numberfield_data = ""
        volumefield_data = ""
        if item == "ID" or item == "ENTRYTYPE":
            pass
        elif item == "year":
            yearfield_data = dict_bib_parsed["year"]
            concate_years = normalizeinput_year_new(yearfield_data)
            if (len(concate_years) != 0):
                temprec["norm_year"] = concate_years
        elif item == "author":
            authorfield_data = dict_bib_parsed["author"]
            concate_author = normalizeinput_author(authorfield_data)
            if (len(concate_author) != 0):
                temprec["norm_author"] = concate_author
        elif item == "title":
            titlefield_data = dict_bib_parsed["title"]
            concate_title = ""
            concate_title = normalizeinput_title_new(re.split('[?.;:!,\"\']',titlefield_data)+[titlefield_data])
            if (len(concate_title) != 0):
                temprec["norm_title"] = concate_title           
        elif item == "pages":
            temprec["norm_pages"] = page_normaliser(dict_bib_parsed["pages"].split(","))
        elif item == "journal":
            temprec["journal"] = normalizeinput_title_new(
                [dict_bib_parsed["journal"].replace("(", " ").replace(")", " ").replace("~", " ")])
        elif item == "volume":
            volumefield_data = re.findall(r'\d+', str(dict_bib_parsed["volume"]))
        elif item == "number":
            numberfield_data = re.findall(r'\d+', str(dict_bib_parsed["number"]))
        elif item == "doi":
            temprec["doi"] = dict_bib_parsed["doi"]
        if numberfield_data != "" or volumefield_data != "":
            if numberfield_data != "":
                if volumefield_data != "":
                    allnumber = str(numberfield_data) + "," + str(volumefield_data)
                else:
                    allnumber = str(numberfield_data)
            else:
                allnumber = str(volumefield_data)
            allnumber = norm_number(allnumber)
            temprec["norm_number"] = allnumber.split(",")
    return (temprec)


def hopefull_dict(bibtex_str):
    dict_ref_parsed = preprocessed_data(bibtex_str)
    list_keys = list(preprocessed_data(bibtex_str).keys())
    new_keys = []
    for item in list_keys:
        new_keys.append(r_dict_tags[item])
    if "title_full" in new_keys:
        new_keys.remove('title_full')
    new_keys.sort()
    rec_ref_keys = str(new_keys).replace("\'", "").replace("[", "").replace("]", "").replace(" ", "")
    try:
        rec_ref_combi = list_combintation["combination_keys"][rec_ref_keys]
    except:
        print(bibtex_str)
    rec_ref_combi.sort(key=len, reverse=True)
    list_hopefull_dict = []
    for item in rec_ref_combi:
        dict_query_solr = dic_query_qen(item, dict_ref_parsed)
        list_hopefull_dict.append(dict_query_solr)
    return (list_hopefull_dict)


def result_solr(temprec, thereshold_fuzzy):
    fqm = finder.query_mapping['add_query']
    for index, query_rec in enumerate(fqm):
        if query_rec["source_field"] == "title":
            for index1, fuzzy_score in enumerate(fqm[index]['query_fields']):
                fqm[index]['query_fields'][index1]["fuzzy"] = thereshold_fuzzy

    finder.query_mapping['add_query'] = fqm
    generated_query = finder.generate_queries(temprec)
    params = {"q": generated_query['our_new_query'].replace("() AND ","")}
    result, url_query = query_solr(solr_url, params, solr_query)
    return result_checker1(result), url_query


def result_for_match(json_input,ref_text_string):
    exclude = set(string.punctuation)
    if "title" in list(json_input.keys()):
         titlefield_data=json_input["title"]
         titlefield_data = ''.join(ch for ch in titlefield_data if ch not in exclude)
    ref_text_string1=''.join(ch for ch in ref_text_string if ch not in exclude)

    list_of_result = []

    if "title" not in list(json_input.keys()):
        titlefield_data=ref_text_string
        json_input["title"]=titlefield_data
        titlefield_data = ''.join(ch for ch in titlefield_data if ch not in exclude)
    if "year" not in list(json_input.keys()):
        json_input["year"]=str(filteryear_new(ref_text_string))
            
    if bool(json_input)==True:
        list_hopefull_dict = hopefull_dict(json_input)
        for item in list_hopefull_dict:
            match_id = "not_match"
            keys_flag = "none"
            if "title" in list(item.keys()):
                thereshold_fuzzy = 0.6
                
                lsofbis=[]
                for itembis in get_bi(titlefield_data):
                    lsofbis.append(" ".join(itembis))
                item["title_full"]=lsofbis
                
                #print(item)
                result_id, url_query = result_solr(item, thereshold_fuzzy)
                if (result_id != "not_match" and result_id != ""):
                    keys_flag = (",".join(item.keys()))
                    match_id = result_id
                    list_of_result.append(
                        {"match_id": match_id, "keys_flag": keys_flag})
                else:
                    keys_flag = (",".join(item.keys()))
                    match_id = "not_match"
                    list_of_result.append(
                        {"match_id": match_id, "keys_flag": keys_flag})
            else:
                result_id, url_query = result_solr(item, thereshold_fuzzy=0.6)
                if (result_id != "not_match" and result_id != ""):
                    keys_flag = (",".join(item.keys()))
                    match_id = result_id
                    list_of_result.append({"match_id": match_id, "keys_flag": keys_flag})
                else:
                    keys_flag = (",".join(item.keys()))
                    match_id = "not_match"
                    list_of_result.append({"match_id": match_id, "keys_flag": keys_flag})

    return list_of_result
