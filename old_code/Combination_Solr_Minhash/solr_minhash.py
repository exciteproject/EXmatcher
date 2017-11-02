import pandas as pd
import time
from sklearn.metrics import jaccard_similarity_score
from scipy import spatial
from solr.solr_search import  main as solrmain
from multiprocessing import Pool

def compare_min_hash(one, two):
    """
    Comparing two hashes
    @return the approximated jaccard distance generated from two hashes
    """
    nr_same = sum([1 for nr in range(len(one)) if
                  one[nr] == two[nr]])

    return float(float(nr_same)/len(one))



def temp_generator(booll,filename,idref,norm_title,sim_score):
    temp_rec = {}
    temp_rec['match'] = booll
    #temp_rec['filename'] = filename
    temp_rec['Id_ref'] = idref
    temp_rec['norm_title_str'] = norm_title
    temp_rec['jaccard'] = sim_score
    return temp_rec


def generate_comparision_rec(maxi,list_ref_h,idx,reult_final_list):
    if maxi['value'] > 0.3:
        temp_rec=temp_generator('true', list_ref_h.get_values()[idx][2], list_ref_h.get_values()[idx][0], list_ref_h.get_values()[idx][3], maxi)
    else:
        temp_rec=temp_generator('false', list_ref_h.get_values()[idx][2], list_ref_h.get_values()[idx][0],list_ref_h.get_values()[idx][3], maxi)
    reult_final_list.append(temp_rec)
    return reult_final_list


def genrate_comparision_score(item,Ref_hash,maxi):
    Solr_item_hash = item[6:]

    error = []
    compare_value=0
    try:
        #compare_value = 1 - spatial.distance.cosine(Ref_hash, Solr_item_hash)
        #compare_value = jaccard_similarity_score(Ref_hash, Solr_item_hash)
        compare_value = compare_min_hash(Ref_hash, Solr_item_hash)
    except:
        error.append(1)
    if compare_value > maxi['value']:
        maxi['id'] = item[0]
        maxi['value'] = compare_value
    return maxi

def generat_data_year(listofyear_prim,lsofyearn,valuelist):
    lsy=set(listofyear_prim).intersection(lsofyearn)
    lsy=list(lsy)
    indexofitem_prime=[]
    for item in lsy:
        indexofitem_prime.append(listofyear_prim.index(item))

    indexofitem_new=[]
    for item in list(set(lsofyearn)-set(lsy)):
        indexofitem_new.append(lsofyearn.index(item))

    listofvalue=[]
    finalitemyear=[]
    for item in indexofitem_prime:
        finalitemyear.append(listofyear_prim[item])
        listofvalue.append(valuelist[item])
    for item in indexofitem_new:
        finalitemyear.append(lsofyearn[item])
        itemtemp=pd.read_csv("./"+lsofyearn[item]+".csv")
        listofvalue.append(itemtemp)
    return listofvalue, finalitemyear

def wirtocsv(reult_final_list,fmx):
    dftr = pd.DataFrame(reult_final_list)
    dftr.to_csv(fmx, index=False,encoding='utf-8-sig')

def wirtoxml(reult_final_list,fmx):
    df = pd.DataFrame(reult_final_list)
    writer = pd.ExcelWriter(fmx, engine='xlsxwriter',encoding='utf-8-sig')
    df.to_excel(writer, sheet_name='Sheet1')
    writer.save()



def main(list_ref_h):
    reult_final_list = []
    counter = 0
    listofyear_prim = []
    valuelist = []
    for idx, item_ref in enumerate(list_ref_h['years']):
        Listofitems_Solr = []
        lsofyearn = []
        if len(eval(item_ref)) > 0:
            for item_nested in eval(item_ref):
                lsofyearn.append(item_nested)
            if (len(lsofyearn) > 0):
                valuelist, listofyear_prim = generat_data_year(listofyear_prim, lsofyearn, valuelist)
            Listofitems_Solrdf = pd.concat(valuelist)
            Listofauthor_ref = eval(list_ref_h['author_name'][idx])
            nullauthorsolr = Listofitems_Solrdf[Listofitems_Solrdf["person_author_normalized_str_mv"].isnull()]
            wauthorsolr = Listofitems_Solrdf[~(Listofitems_Solrdf["person_author_normalized_str_mv"].isnull())]
            Listofitems_Solr = []

            for itemar_nes in Listofauthor_ref:
                if len(itemar_nes) > 1:
                    Listofitems_Solr.append(
                        wauthorsolr[wauthorsolr["person_author_normalized_str_mv"].str.contains(itemar_nes)])
            try:
                Listofitems_Solrdf = pd.concat(Listofitems_Solr)
            except ValueError, Argument:
                reult_final_list.append(
                    temp_generator('False', '', list_ref_h.get_values()[idx][0], list_ref_h.get_values()[idx][3],
                                   'Not_Sim'))
            Ref_hash = eval(list_ref_h.ix[[idx]]["hash_values"].get_values()[0])
            maxi = {'id': '', 'value': 0}
            for item in Listofitems_Solrdf.get_values():
                maxi = genrate_comparision_score(item, Ref_hash, maxi)
            reult_final_list = generate_comparision_rec(maxi, list_ref_h, idx, reult_final_list)
            counter = counter + 1
        else:
            Normlized_title_author = []
            temprec = {}
            temprec["norm_title_str"] = list_ref_h['norm_title_str'][idx]
            temprec["author_name"] = eval(list_ref_h['author_name'][idx])
            Normlized_title_author.append(temprec)
            Flag_match, sowiportid = solrmain(Normlized_title_author)
            temp_rec = temp_generator(Flag_match, list_ref_h.get_values()[idx][2], list_ref_h.get_values()[idx][0],
                                      list_ref_h.get_values()[idx][3], {'id': sowiportid, 'value': 'solr'})
            reult_final_list.append(temp_rec)
            counter = counter + 1

        #print counter
    return reult_final_list





def main2(item_loop):
        reult_final_list = []
        global counter
        global listofyear_prim
        global valuelist
        global list_ref_h
        idx=item_loop[0]
        item_ref=item_loop[1]

        lsofyearn = []
        if len(eval(item_ref)) > 0:
            for item_nested in eval(item_ref):
                lsofyearn.append(item_nested)
            if (len(lsofyearn) > 0):
                valuelist, listofyear_prim = generat_data_year(listofyear_prim, lsofyearn, valuelist)
            Listofitems_Solrdf = pd.concat(valuelist)
            Listofauthor_ref = eval(list_ref_h['author_name'][idx])
            #nullauthorsolr = Listofitems_Solrdf[Listofitems_Solrdf["person_author_normalized_str_mv"].isnull()]
            wauthorsolr = Listofitems_Solrdf[~(Listofitems_Solrdf["person_author_normalized_str_mv"].isnull())]
            Listofitems_Solr = []

            for itemar_nes in Listofauthor_ref:
                if len(itemar_nes) > 1:
                    Listofitems_Solr.append(
                        wauthorsolr[wauthorsolr["person_author_normalized_str_mv"].str.contains(itemar_nes)])
            try:
                Listofitems_Solrdf = pd.concat(Listofitems_Solr)
            except ValueError, Argument:
                reult_final_list.append(
                    temp_generator('False', '', list_ref_h.get_values()[idx][0], list_ref_h.get_values()[idx][3],
                                   'Not_Sim'))
            Ref_hash = eval(list_ref_h.ix[[idx]]["hash_values"].get_values()[0])
            maxi = {'id': '', 'value': 0}
            for item in Listofitems_Solrdf.get_values():
                maxi = genrate_comparision_score(item, Ref_hash, maxi)
            reult_final_list = generate_comparision_rec(maxi, list_ref_h, idx, reult_final_list)
            counter = counter + 1
        else:
            Normlized_title_author = []
            temprec = {}
            temprec["norm_title_str"] = list_ref_h['norm_title_str'][idx]
            temprec["author_name"] = eval(list_ref_h['author_name'][idx])
            Normlized_title_author.append(temprec)
            Flag_match, sowiportid = solrmain(Normlized_title_author)
            temp_rec = temp_generator(Flag_match, list_ref_h.get_values()[idx][2], list_ref_h.get_values()[idx][0],
                                      list_ref_h.get_values()[idx][3], {'id': sowiportid, 'value': 'solr'})
            reult_final_list.append(temp_rec)
            counter = counter + 1

        return reult_final_list[-1]



reult_final_list = []
counter = 0
listofyear_prim = []
valuelist = []
list_ref_h = pd.read_csv("./list_ref_hashes_norm.csv")
if __name__ == '__main__':
    reult_final_list1 = []
    list_ref_h = list_ref_h.sort_values(['years'])
    list_ref_h = list_ref_h[~(list_ref_h["norm_title_str"].isnull())]
    list_ref_h = list_ref_h.reset_index(drop=True)

    t0= time.time()
    reult_final_list=main(list_ref_h)
    t1 = time.time()
    total = t1 - t0
    print total



    #t0 = time.time()
    #p = Pool(2)
    #job_args = [(idx, item_ref) for idx, item_ref in enumerate(list_ref_h['years'])]
    #reult_final_list1=p.map(main2, job_args)
    #t1 = time.time()
    #total = t1 - t0
    #print total
    #print 'this is result',len(reult_final_list1)
    wirtocsv(reult_final_list, fmx="./reult.csv")