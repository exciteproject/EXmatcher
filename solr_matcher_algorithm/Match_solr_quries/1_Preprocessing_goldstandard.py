# run with python27
# -*- coding: UTF-8 -*-

import pandas as pd
import bibtexparser
import itertools
import re
import string
import numpy as np


def bib_pars(item1):
    bib_database = bibtexparser.loads(item1)
    return (bib_database.entries[0])

def findsubsets(S,m):
    return list(itertools.combinations(S, m))

def filteryear(itemyear):
    year = ''
    try:
        if 1000<int(itemyear) and 3000>int(itemyear):
            year=itemyear
    except:
        pass
    return year

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


def norm_number(stringnumber):
    finalreturn=[]
    listofnumber=stringnumber.split(",")
    for item in listofnumber:
        finalreturn=finalreturn+re.findall('\d+', item )
    return ",".join(list(set(finalreturn)))


##################################[  Step 1   ]###################################

# 1 -read data from gold standard - manually generated - into df variable

# 2 -generated a csv "table_orgin_data.csv" file which defines which table is the origin of each record
### (table1 - from matched ref strings to sowiport id and table2 - from whole reference strings).

# 3- made a dataframe whichs contain ref_ids and all matches for each - saved into variable "match_id"

# 4- made a richer dataframe by using "match_id" and information in bibtex_table that we had. - the result is stored in "list_of_bibtex_ref"
### it contains this list as its column ["ref_id","ref_text","bibtex","dict","year","journal","volume","number","doi","pmid","pages","author","title"]

##################################################################################

df=pd.read_csv("Data/sowiIdDuplicatesEdit.csv",sep=";")
df[["ref_id","original_table","sowiport_id"]].to_csv("Data/saved/table_orgin_data.csv",index=False, encoding='utf-8')

match_id=df[["ref_id","sowiport_id","sowi_IdDuplicates"]]
match_id["sowiport_id1"] = np.nan
for item in match_id["ref_id"]:
    ls_temp=[]
    si=list(match_id[match_id["ref_id"]==item]["sowiport_id"])[0]
    if (si!="not_match"):
        ls_temp.append(si.strip())
    si=list(match_id[match_id["ref_id"]==item]["sowi_IdDuplicates"])[0].replace("[","").replace("]","").split(",")
    for item1 in si:
        if item1!="null":
            ls_temp.append(item1.strip())
    match_id.loc[match_id[match_id["ref_id"]==item].iloc[0].name,"sowiport_id1"]=str(ls_temp)

match_id=match_id[["ref_id","sowiport_id1"]]

bibtex_csv=pd.read_csv("Data/bibtex_19oct.csv",header=None)

ref_ex=df["ref_text"]

id=df["ref_id"]

list_of_bibtex_ref=[]
for item in id:
    temp=[]
    temp.append(item)
    temp.append(list(df[df["ref_id"]==item]["ref_text"])[0])
    temp.append(list(bibtex_csv[bibtex_csv[0]==item][1])[0])
    temp.append(bib_pars(list(bibtex_csv[bibtex_csv[0]==item][1])[0]))
    list_of_bibtex_ref.append(temp)

list_of_bibtex_ref=pd.DataFrame(list_of_bibtex_ref)
listofkeys=['year', 'journal', 'volume', 'number', 'doi', 'pmid', 'pages', 'author', 'title']
for item in listofkeys:
    list_of_bibtex_ref[item] = np.nan

for item in list_of_bibtex_ref[0]:
    temp_key=""
    for item1 in list(list_of_bibtex_ref[list_of_bibtex_ref[0]==item][3])[0].keys():
        if item1!="ENTRYTYPE" and item1!="ID":
            value_instance=list(list_of_bibtex_ref[list_of_bibtex_ref[0]==item][3])[0][item1]
            list_of_bibtex_ref.loc[list_of_bibtex_ref[list_of_bibtex_ref[0]==item].iloc[0].name,item1]=value_instance
            if list(list_of_bibtex_ref[list_of_bibtex_ref[0]==item][item1].isnull())[0]==False:
                temp_key=temp_key+","+item1
            #list_of_bibtex_ref[list_of_bibtex_ref[0]==item][item1]=value_instance


list_of_bibtex_ref.to_csv("Data/saved/richbibtex.csv", encoding='utf-8')

list_of_bibtex_ref.columns =["ref_id","ref_text","bibtex","dict","year","journal","volume","number","doi","pmid","pages","author","title"]
list_of_bibtex_ref=pd.DataFrame(list_of_bibtex_ref)
list_of_bibtex_ref=pd.merge(list_of_bibtex_ref, match_id, on='ref_id')


##################################[  Step 2   ]###################################

# 5 - in this step we enrich "list_of_bibtex_ref" by adding normalized data
### these fields are added ["norm_year", "norm_author", "norm_title", "norm_pages", "norm_number"

##################################################################################

list_of_bibtex_ref["norm_year"] = np.nan
list_of_bibtex_ref["norm_author"] = np.nan
list_of_bibtex_ref["norm_title"] = np.nan
list_of_bibtex_ref["norm_pages"] = np.nan
list_of_bibtex_ref["norm_number"] = np.nan

for item in list_of_bibtex_ref["ref_id"]:
    yearfield_data = list(list_of_bibtex_ref[list_of_bibtex_ref["ref_id"] == item]["year"])[0]
    authorfield_data = list(list_of_bibtex_ref[list_of_bibtex_ref["ref_id"] == item]["author"])[0]
    titlefield_data = list(list_of_bibtex_ref[list_of_bibtex_ref["ref_id"] == item]["title"])[0]
    pagesfield_data = list(list_of_bibtex_ref[list_of_bibtex_ref["ref_id"] == item]["pages"])[0]

    volumefield_data = list(list_of_bibtex_ref[list_of_bibtex_ref["ref_id"] == item]["volume"])[0]
    numberfield_data = list(list_of_bibtex_ref[list_of_bibtex_ref["ref_id"] == item]["number"])[0]

    concate_years = ",".join(normalizeinput_year(yearfield_data))
    concate_author = ",".join(normalizeinput_author(authorfield_data))
    concate_title = normalizeinput_title(titlefield_data)
    if (concate_years.strip() != "" and concate_years.strip() != "nan"):
        list_of_bibtex_ref.loc[
            list_of_bibtex_ref[list_of_bibtex_ref["ref_id"] == item].iloc[0].name, "norm_year"] = concate_years
    if (concate_author.strip() != "" and concate_author.strip() != "nan"):
        list_of_bibtex_ref.loc[
            list_of_bibtex_ref[list_of_bibtex_ref["ref_id"] == item].iloc[0].name, "norm_author"] = concate_author
    if (concate_title.strip() != "" and concate_title.strip() != "nan"):
        list_of_bibtex_ref.loc[
            list_of_bibtex_ref[list_of_bibtex_ref["ref_id"] == item].iloc[0].name, "norm_title"] = concate_title
    if (str(pagesfield_data) != 'nan' and '-' in pagesfield_data):
        list_of_bibtex_ref.loc[
            list_of_bibtex_ref[list_of_bibtex_ref["ref_id"] == item].iloc[0].name, "norm_pages"] = str(
            pagesfield_data).replace("--", "-")

    if (str(volumefield_data) == 'nan'):
        volumefield_data = ""
    if (str(numberfield_data) == 'nan'):
        numberfield_data = ""
    if numberfield_data != "" or volumefield_data != "":
        if numberfield_data != "":
            if volumefield_data != "":
                allnumber = str(numberfield_data) + "," + str(volumefield_data)
            else:
                allnumber = str(numberfield_data)
        else:
            allnumber = str(volumefield_data)
        allnumber = norm_number(allnumber)
        list_of_bibtex_ref.loc[
            list_of_bibtex_ref[list_of_bibtex_ref["ref_id"] == item].iloc[0].name, "norm_number"] = allnumber





##################################[  Step 3   ]###################################

# 6 - in this step we enrich "list_of_bibtex_ref". We made the column "dict_norm1", the data in this field is a dictionary of fields and their norm value for each.
### also in we add "subset_data_query", it contains a list of possible dictionary out of whole existing fields (all subsets of whole existing fields for a record)

##################################################################################

list_fields=["journal","doi","norm_number","norm_year","norm_author","norm_title","norm_pages"]
dict_fields={"journal":"journal","doi":"doi","norm_number":"number","norm_year":"year","norm_author":"author","norm_title":"title","norm_pages":"pages"}

list_of_bibtex_ref["norm_listoffields"] = np.nan
list_of_bibtex_ref["norm_dict"] = np.nan

for item in list_of_bibtex_ref["ref_id"]:
    dic_norm={}
    temp_key=""
    for item1 in list_fields:
            if list(list_of_bibtex_ref[list_of_bibtex_ref["ref_id"]==item][item1].isnull())[0]==False:
                temp_key=temp_key+","+dict_fields[item1]
                dic_norm[item1]=list(list_of_bibtex_ref[list_of_bibtex_ref["ref_id"]==item][item1])[0]
    list_of_bibtex_ref.loc[list_of_bibtex_ref[list_of_bibtex_ref["ref_id"]==item].iloc[0].name,"norm_listoffields"]=temp_key
    list_of_bibtex_ref.loc[list_of_bibtex_ref[list_of_bibtex_ref["ref_id"]==item].iloc[0].name,"norm_dict"]=str(dic_norm)
            #list_of_bibtex_ref[list_of_bibtex_ref[0]==item][item1]=value_instance


list_of_bibtex_ref["norm_subsets"] = np.nan
for item in list_of_bibtex_ref["ref_id"]:
    listofmeta=list(list_of_bibtex_ref[list_of_bibtex_ref["ref_id"]==item]["norm_listoffields"])[0].split(",")[1:]
    cl=[]
    max_len=len(listofmeta)+1
    #print(listofmeta)
    if max_len>1:
        for i in range(1,max_len):
            cl.append(findsubsets(listofmeta,i))
    lisofsubset = list(itertools.chain(*cl))
    #print (list(lisofsubset))
    list_of_bibtex_ref.loc[list_of_bibtex_ref[list_of_bibtex_ref["ref_id"]==item].iloc[0].name,"norm_subsets"]=str(list(lisofsubset))
    #print("======")


data_ref_norm=list_of_bibtex_ref[["ref_id","ref_text","journal","doi","sowiport_id1","norm_year","norm_author","norm_title","norm_pages","norm_number","norm_listoffields","norm_dict","norm_subsets"]]
data_ref_norm["dic_norm1"] = np.nan

for item in data_ref_norm["ref_id"]:
    temp_dict={}
    for item1 in eval(list(data_ref_norm[data_ref_norm["ref_id"]==item]["norm_dict"])[0]).keys():
        if item1=="norm_year" or item1=="norm_author" or item1=="norm_author" or item1=="norm_number" or item1=="norm_pages":
            value_instance=eval(list(data_ref_norm[data_ref_norm["ref_id"]==item]["norm_dict"])[0])[item1]
            list_value=value_instance.split(",")
            temp_dict[item1]=list_value
        else:
            value_instance=eval(list(data_ref_norm[data_ref_norm["ref_id"]==item]["norm_dict"])[0])[item1]
            temp_dict[item1]=value_instance
    data_ref_norm.loc[data_ref_norm[data_ref_norm["ref_id"]==item].iloc[0].name,"dic_norm1"]=str(temp_dict)


data_ref_norm.head(1)["dic_norm1"][0]

r_dict_fields={"journal":"journal","doi":"doi","number":"norm_number","year":"norm_year","author":"norm_author","title":"norm_title","pages":"norm_pages"}

data_ref_norm["subset_data_query"] = np.nan

for item in data_ref_norm["ref_id"]:
    lsofsubdict=[]
    for item1 in eval(list(data_ref_norm[data_ref_norm["ref_id"]==item]["norm_subsets"])[0]):
        subset_temp={}
        for item2 in item1:
            subset_temp[item2]=eval(list(data_ref_norm[data_ref_norm["ref_id"]==item]["dic_norm1"])[0])[r_dict_fields[item2]]
        lsofsubdict.append(subset_temp)
    data_ref_norm.loc[data_ref_norm[data_ref_norm["ref_id"]==item].iloc[0].name,"subset_data_query"]=(str(lsofsubdict))





##################################[  Step 4   ]###################################

# 7 - save all possible subsets dictionary for a record
# 8 - save the preprocessed data

##################################################################################


sdq=data_ref_norm[["ref_id","subset_data_query"]]
listofallsdq=[]
for item in sdq["ref_id"]:
    for item1 in eval(list(sdq[sdq["ref_id"]==item]["subset_data_query"])[0]):
        templist={}
        templist["ref_id"]=item
        templist["subset_data_query"]=item1
        listofallsdq.append(templist)


sdq=pd.DataFrame(listofallsdq)
sdq.to_csv("Data/saved/sdq.csv",index=None, encoding='utf-8')
data_ref_norm.to_csv("Data/saved/data_ref_norm.csv",index=None, encoding='utf-8')



