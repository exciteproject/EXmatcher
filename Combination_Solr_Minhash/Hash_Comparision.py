# -*- coding: UTF-8 -*-
__author__ = "Behnam Ghavimi"
import pandas as pd
from sklearn.metrics import jaccard_similarity_score
from scipy import spatial
from solr.solr_search import  main as solrmain
import time

def shift(l, n):
    return l[n:] + l[:n]

def shiftarray(lstshif,item,valuelist):
    # [#,#,#]#
    if len(valuelist)!=0:
        if item in lstshif[1:4]:
            if item == lstshif[1]:
                pass
            else:
                index=lstshif.index(item)
                lent=len(lstshif)
                lstshif=lstshif[0:1]+lstshif[index:index+1]+lstshif[1:index]+lstshif[index+1:lent]
                lstshif[0]=0
                lstshif[4]=0
                valuelist=valuelist[index-1:index]+valuelist[0:index-1]+valuelist[index:lent-2]
        else:
            lstshif[0]=item
            lstshif=shift(lstshif,-1)
            lstshif[0]=0
            lstshif[4]=0
            valuelist[2]=valuelist[1]
            valuelist[1]=valuelist[0]
            valuelist[0]=pd.read_csv("./Data_split/"+item+".csv")
    else:
        itemtemp=pd.read_csv("./Data_split/"+item+".csv")
        valuelist=[itemtemp,itemtemp,itemtemp]
        lstshif[1]=item
    return valuelist,lstshif


def compare_min_hash(one, two):
    """
    Comparing two hashes
    @return the approximated jaccard distance generated from two hashes
    """
    nr_same = sum([1 for nr in range(len(one)) if
                  one[nr] == two[nr]])

    return float(float(nr_same)/len(one))


def wirtocsv(reult_final_list,fmx="result.csv"):
    dftr = pd.DataFrame(reult_final_list)
    dftr.to_csv(fmx, index=False)

def wirtoxml(reult_final_list,fmx):
    df = pd.DataFrame(reult_final_list)
    writer = pd.ExcelWriter(fmx, engine='xlsxwriter')
    df.to_excel(writer, sheet_name='Sheet1')
    writer.save()

def temp_generator(bool,filename,idref,norm_title,sim_score):
    temp_rec = {}
    temp_rec['match'] = bool
    #temp_rec['filename'] = filename
    temp_rec['Id_ref'] = idref
    temp_rec['norm_title_str'] = norm_title
    temp_rec['jaccard'] = sim_score
    return temp_rec

def generate_comparision_rec(maxi,list_ref_h,idx,reult_final_list):
    if maxi['value'] > 0.7:
        temp_rec=temp_generator('true', list_ref_h.get_values()[idx][2], list_ref_h.get_values()[idx][0], list_ref_h.get_values()[idx][3], maxi)
    else:
        temp_rec=temp_generator('false', list_ref_h.get_values()[idx][2], list_ref_h.get_values()[idx][0],list_ref_h.get_values()[idx][3], maxi)
    reult_final_list.append(temp_rec)
    return reult_final_list

def genrate_comparision_score(item,Ref_hash,maxi):
    Solr_item_hash = item[6:]
    # compare_value=compare_min_hash(ref_hash_list,vector_solr_item)
    # compare_value=jaccard_similarity_score(ref_hash_list,vector_solr_item)
    error = []
    compare_value=0
    try:
        compare_value = 1 - spatial.distance.cosine(Ref_hash, Solr_item_hash)
    except:
        error.append(1)
    if compare_value > maxi['value']:
        maxi['id'] = item[0]
        maxi['value'] = compare_value
    return maxi

def main():
    t0 = time.time()
    list_ref_h = pd.read_csv("./Gold_Standard_Wp2/manual/list_ref_hashes_norm.csv")
    reult_final_list = []
    list_ref_h=list_ref_h.sort_values(['years'])
    list_ref_h = list_ref_h[~(list_ref_h["norm_title_str"].isnull())]
    list_ref_h = list_ref_h.reset_index(drop=True)

    counter = 0
    listofyear = [0, 0, 0, 0, 0]
    valuelist = []
    for idx, item_ref in enumerate(list_ref_h['years']):
        Listofitems_Solr = []
        if len(eval(item_ref)) > 0:
            for item_nested in eval(item_ref):
                if item_nested != '':
                    valuelist, listofyear = shiftarray(listofyear, item_nested, valuelist)
            for itemnn in valuelist:
                Listofitems_Solr.append(itemnn)
            Listofitems_Solrdf = pd.concat(Listofitems_Solr)
            Listofauthor_ref = eval(list_ref_h['author_name'][idx])
            nullauthorsolr = Listofitems_Solrdf[Listofitems_Solrdf["person_author_normalized_str_mv"].isnull()]
            wauthorsolr = Listofitems_Solrdf[~(Listofitems_Solrdf["person_author_normalized_str_mv"].isnull())]
            Listofitems_Solr = []
            #Listofitems_Solr.append(nullauthorsolr)
            for itemar_nes in Listofauthor_ref:
                if len(itemar_nes) > 1:
                    Listofitems_Solr.append(
                        wauthorsolr[wauthorsolr["person_author_normalized_str_mv"].str.contains(itemar_nes)])
            try:
                Listofitems_Solrdf = pd.concat(Listofitems_Solr)
            except:
                print item_ref
            Ref_hash = eval(list_ref_h.ix[[idx]]["hash_values"].get_values()[0])
            maxi = {'id': '', 'value': 0}
            for item in Listofitems_Solrdf.get_values():
                maxi=genrate_comparision_score(item, Ref_hash, maxi)

            reult_final_list=generate_comparision_rec(maxi, list_ref_h, idx, reult_final_list)
            counter = counter + 1
            print counter
        else:
            Normlized_title_author=[]
            temprec = {}
            temprec["norm_title_str"] = list_ref_h['norm_title_str'][idx]
            temprec["author_name"] = eval(list_ref_h['author_name'][idx])
            Normlized_title_author.append(temprec)
            Flag_match,sowiportid=solrmain(Normlized_title_author)
            temp_rec=temp_generator(Flag_match, list_ref_h.get_values()[idx][2], list_ref_h.get_values()[idx][0], list_ref_h.get_values()[idx][3], {'id':sowiportid,'value':'solr'})
            reult_final_list.append(temp_rec)
            counter = counter + 1
            print counter
    t1 = time.time()

    total = t1 - t0
    print(total)

    wirtocsv(reult_final_list, fmx="./manual_result/reult.csv")
    wirtoxml(reult_final_list, fmx="./manual_result/reult.xlsx")

if __name__ == '__main__':
    main()