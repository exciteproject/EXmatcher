# -*- coding: UTF-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import pandas as pd
import bibtexparser
import itertools
import re
import string
import numpy as np
from query_builder import SOLRMappingQueryBuilder
import urllib
from urllib import urlopen


dict_tags={"journal":"journal","doi":"doi","number":"norm_number","year":"norm_year","author":"norm_author","title":"norm_title","pages":"norm_pages"}
r_dict_tags={"journal":"journal","doi":"doi","norm_number":"number","norm_year":"year","norm_author":"author","norm_title":"title","norm_pages":"pages"}


solr_url = ""
finder = SOLRMappingQueryBuilder("./Mapper.json")
solr_query = {'fl': 'id', 'rows': '1', 'start': '0', 'wt': 'python','api_key':''}


def normalizeinput_title(titlestr):
    try:
        titlestr = str(float(titlestr))
    except:
        pass

    normalizedtitle = "".join(l for l in titlestr if l not in string.punctuation)
    normalizedtitle = normalizedtitle.lower()
    normalizedtitle = normalizedtitle.replace(" ", "")
    normalizedtitle=normalizedtitle.replace(u'ü', 'u')
    normalizedtitle=normalizedtitle.replace(u'ä', 'a')
    normalizedtitle=normalizedtitle.replace(u'ö', 'o')
    normalizedtitle=normalizedtitle.replace(u'ß', 'ss')
    normalizedtitle =''.join([i if ord(i) < 128 else '' for i in normalizedtitle])
    return normalizedtitle


def normalizeinput_author(autors):
    try:
        autors=str(float(autors))
    except:
        pass

    autors = autors.split(',')
    norm_authors=[]
    for item1 in autors:
        norm_item_iter=filterauthor(item1)
        if len(norm_item_iter)>1:
            norm_authors.append(norm_item_iter)
    return norm_authors


def filterauthor(item):
    normalizedtitle = "".join(l for l in item if l not in string.punctuation)
    normalizedtitle = normalizedtitle.lower()
    normalizedtitle= normalizedtitle.strip()
    normalizedtitle = normalizedtitle.replace(" ", "")

    normalizedtitle=normalizedtitle.replace(u'ü', '')
    normalizedtitle=normalizedtitle.replace(u'ä', '')
    normalizedtitle=normalizedtitle.replace(u'ö', '')
    normalizedtitle=normalizedtitle.replace(u'ß', 'ss')
    normalizedtitle =''.join([i if ord(i) < 128 else '' for i in normalizedtitle])
    return normalizedtitle

def normalizeinput_year(Year_item):
        Year_item = str(Year_item)
        listofyear = []
        listof_replace = re.findall('[a-zA-Z]', Year_item)
        listof_replace = list(set(listof_replace))
        for item in listof_replace:
            Year_item = Year_item.replace(item, " ")
        for itemyis in Year_item.split(','):
            for itemyis_s in itemyis.split(' '):
                item_iter_year = filteryear(itemyis_s)
                if item_iter_year != '':
                    listofyear.append(item_iter_year)
        return listofyear

def norm_number(stringnumber):
    finalreturn=[]
    listofnumber=stringnumber.split(",")
    for item in listofnumber:
        finalreturn=finalreturn+re.findall('\d+', item )
    return ",".join(list(set(finalreturn)))


def filteryear(itemyear):
    year = ''
    try:
        if 1000<int(itemyear) and 3000>int(itemyear):
            year=itemyear
    except:
        pass
    return year


def bib_pars(item1):
    bib_database = bibtexparser.loads(item1)
    return (bib_database.entries[0])

#this function has bug but not remove as a back-up code - temporary
def preprocessed_data_old(bibtex_str):
    dict_bib_parsed = bib_pars(bibtex_str)
    dict_bib_keys = dict_bib_parsed.keys()

    temprec = {}
    for item in dict_bib_keys:
        numberfield_data = ""
        volumefield_data = ""
        if item == "ID" or item == "ENTRYTYPE":
            pass
        elif item == "year":
            yearfield_data = dict_bib_parsed["year"]
            concate_years = ",".join(normalizeinput_year(yearfield_data))
            # --
            if (concate_years.strip() != ""):
                temprec["norm_year"] = concate_years
        elif item == "author":
            authorfield_data = dict_bib_parsed["author"]
            concate_author = ",".join(normalizeinput_author(authorfield_data))
            # --
            if (concate_author.strip() != ""):
                temprec["norm_author"] = concate_author
        elif item == "title":
            titlefield_data = dict_bib_parsed["title"]
            concate_title = ""
            concate_title = normalizeinput_title(titlefield_data)
            # --
            if (concate_title.strip() != ""):
                temprec["norm_title"] = concate_title
        elif item == "pages":
            pagesfield_data = str(dict_bib_parsed["pages"]).replace("--", "-")
            # --
            if ('-' in pagesfield_data):
                newlist_pages = []
                for item_page in pagesfield_data.split(','):
                    temp_page_item = ""
                    for token_item in item_page:
                        if token_item.isdigit() or token_item == "-":
                            temp_page_item += token_item
                    temp_page_item=temp_page_item.replace("--","-")
                    if temp_page_item[0]=="-":
                        temp_page_item=temp_page_item[1:]
                    if len(list(set(temp_page_item)))>1 and '-' in temp_page_item:
                        newlist_pages.append(temp_page_item)
                temprec["norm_pages"] = newlist_pages        
                
        elif item == "journal":
            # --
            temprec["journal"] = dict_bib_parsed["journal"]

        elif item == "volume":
            volumefield_data = dict_bib_parsed["volume"]

        elif item == "number":
            numberfield_data = dict_bib_parsed["number"]

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
            # --
            temprec["norm_number"] = allnumber
    return (temprec)


def normalizeinput_year_new(Year_item):
    Year_item = str(Year_item)
    listofyear = []
    listof_replace = re.findall('[a-zA-Z]', Year_item)
    listof_replace = list(set(listof_replace))
    for item in listof_replace:
        Year_item = Year_item.replace(item, " ")
    for itemyis in Year_item.split(','):
        for itemyis_s in itemyis.split(' '):
            item_iter_year = filteryear(itemyis_s)
            if item_iter_year != '':
                listofyear.append(item_iter_year.replace(" ",""))
    return listofyear    
    
def preprocessed_data(bibtex_str):
    dict_bib_parsed = bib_pars(bibtex_str)
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
            # --
            if (len(concate_years) != 0):
                temprec["norm_year"] = concate_years
        elif item == "author":
            authorfield_data = dict_bib_parsed["author"]
            concate_author = normalizeinput_author(authorfield_data)
            # --
            if (len(concate_author) != 0):
                temprec["norm_author"] = concate_author
        elif item == "title":
            titlefield_data = dict_bib_parsed["title"]
            concate_title = ""
            concate_title = normalizeinput_title(titlefield_data)
            # --
            if (concate_title.strip() != ""):
                temprec["norm_title"] = concate_title
        elif item == "pages":
            pagesfield_data = str(dict_bib_parsed["pages"]).replace("--", "-")
            # --
            if ('-' in pagesfield_data):
                newlist_pages = []
                for item_page in pagesfield_data.split(','):
                    temp_page_item = ""
                    for token_item in item_page:
                        if token_item.isdigit() or token_item == "-":
                            temp_page_item += token_item
                    temp_page_item=temp_page_item.replace("--","-")
                    if temp_page_item[0]=="-":
                        temp_page_item=temp_page_item[1:]
                    if len(list(set(temp_page_item)))>1 and '-' in temp_page_item:
                        newlist_pages.append(temp_page_item)
                temprec["norm_pages"] = newlist_pages        
                
        elif item == "journal":
            # --
            temprec["journal"] = dict_bib_parsed["journal"]

        elif item == "volume":
            volumefield_data = re.findall(r'\d+',dict_bib_parsed["volume"])

        elif item == "number":
            numberfield_data = re.findall(r'\d+',dict_bib_parsed["number"])

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
            # --
            temprec["norm_number"] = allnumber.split(",")
    return (temprec)

def dic_query_qen(item_ref_combi,dict_ref_parsed):
    temp={}
    for item in item_ref_combi:
        temp[item]=dict_ref_parsed[dict_tags[item]]
    return temp


def matcher_query(bibtex_str):
    dict_ref_parsed = preprocessed_data(bibtex_str)
    list_keys = list(preprocessed_data(bibtex_str).keys())
    new_keys = []
    for item in list_keys:
        new_keys.append(r_dict_tags[item])
    new_keys.sort()
    rec_ref_keys = str(new_keys).replace("\'", "").replace("[", "").replace("]", "").replace(" ", "")
    rec_ref_combi = list_combintation["combination_keys"][rec_ref_keys]
    rec_ref_combi.sort(key=len, reverse=True)
    return (rec_ref_combi)


def preprocess_dict_query(dict_query):
    list_combintation=[]
    for item in dict_query.iterrows():
        tem_rec=[]
        used_keys=str(item[1]["used_keys"]).replace("\"","").replace("'","").replace("(","").replace(")","").replace(" ","").split(",")
        if "" in used_keys:
            used_keys.remove("")
        used_keys.sort()
        used_keys=(str(used_keys).replace("\'","").replace("[","").replace("]","").replace(" ",""))
        tem_rec.append(used_keys)
        combination_keys=eval(str(item[1]["combination"]).replace("\\","").replace("'","").replace(" ",""))
        tem_rec.append(combination_keys)
        list_combintation.append(tem_rec)
    list_combintation=pd.DataFrame(list_combintation)
    return (list_combintation)

def result_solr(temprec):
    generated_query = finder.generate_queries(temprec)
    params = {"q": generated_query['title_our_new_query']}
    result,url_query = query_solr(solr_url, params, solr_query)
    return result_checker(result),url_query

def result_checker(result):
    if (len(result['response']['docs']) != 0):
        result_match = result['response']['docs'][0]['id']
    else:
        result_match='not_match'
    return result_match


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


def hopefull_dict(bibtex_str):
    dict_ref_parsed=preprocessed_data(bibtex_str)
    list_keys=list(preprocessed_data(bibtex_str).keys())
    new_keys=[]
    for item in list_keys:
        new_keys.append(r_dict_tags[item])
    new_keys.sort()
    list_hopefull_dict = []
    if new_keys:
        rec_ref_keys=str(new_keys).replace("\'","").replace("[","").replace("]","").replace(" ","")
        rec_ref_combi=list_combintation["combination_keys"][rec_ref_keys]
        rec_ref_combi.sort(key=len, reverse=True)
        for item in rec_ref_combi:
            dict_query_solr=dic_query_qen(item,dict_ref_parsed)
            list_hopefull_dict.append(dict_query_solr)
    return (list_hopefull_dict)


dict_query=pd.read_csv("../Match_solr_quries/query_selection/Data/Dictionary_query/precision_09_dict_query.csv")
list_combintation=preprocess_dict_query(dict_query)
list_combintation.columns=["field_key","combination_keys"]
list_combintation.set_index("field_key",inplace=True)


def result_for_match(bibtex_str):
    try:
        match_id="not_match"
        keys_flag="none"
        list_hopefull_dict=hopefull_dict(bibtex_str)
        for item in list_hopefull_dict:
                    match_id="not_match"
                    keys_flag="none"
                    result_id,url_query=result_solr(item)
                    if (result_id!="not_match" and result_id!=""):
                            keys_flag=(",".join(item.keys()))
                            match_id=(result_id)
                            break;
    except Exception as e:
        #match_id="error"
        match_id = "not_match"
    return match_id,keys_flag


#bibtex_str='@article{Jacob1991,\nauthor = {Jacob, Bertot J., A.},\njournal = {Montréal: Méridien},\ntitle = {Intervenir avec les immigrants et les réfugiés, (ref, Union européenne, (gen},\nyear = {1991, 2008},\n}\n'
#print(result_for_match(bibtex_str))