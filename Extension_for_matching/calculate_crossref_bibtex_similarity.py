# -*- coding: utf-8 -*-
import re
import sys
import ast
import numpy as np
import pandas as pd
import string
import random
import json
import psycopg2
import binascii
import datetime
import traceback
import pandas.io.sql as pd_psql

"""
author: Haydar Akyürek

description:
Python 3 script to calculate similarity of crossref data with 'source' data.
Values retrieved from database dbexcitetest, tables: crossref_references_ha and join_ref_bibtex_match_ha
result dataframe saved as csv "crossrefvalue.csv"


run with python3 calculate_crossref_bibtex_similarity.py
BEFORE running: input login data for database access in line 28.

"""
next_prime = 4294967311
max_shingle_id = 2**32-1


def save_results_to_db(psql_login_data, dataframe):
    try:
        conn = psycopg2.connect(dbname=psql_login_data[0], user=psql_login_data[1], host=psql_login_data[2], password=psql_login_data[3])
    except psycopg2.Error as e:
        print ("Unable to connect!")
        print (e)
        sys.exit(1)
    cursor = conn.cursor()
    result_df = dataframe.loc[~dataframe['final_score']==False]
    result_df = dataframe
    #list_of_result_tuples = []
    for ref_id_index, row in result_df.iterrows():
        val = (int(str(ref_id_index)), str(row['checkdoi']), str(row['autscore']), str(row['title_score_85']), str(row['yearscore']))
        #list_of_result_tuples.append(val)

        cursor.execute('INSERT INTO similarity_matches_ha (ref_id, query_doi, query_author, query_title, query_year) VALUES '
                       '(%s, %s, %s, %s, %s)', val)

    conn.commit()
    conn.close()


def save_match_dict_to_db(psql_login_data, result_df):
    try:
        conn = psycopg2.connect(dbname=psql_login_data[0], user=psql_login_data[1], host=psql_login_data[2], password=psql_login_data[3])
    except psycopg2.Error as e:
        print ("Unable to connect!")
        print (e)
        sys.exit(1)
    cursor = conn.cursor()

    #value_dict = result_df.to_dict('index')




    for ref_id_index, row in result_df.iterrows():
        val_dict = {
        'autscore' : row['autscore'],
        'yearscore': row['yearscore'],
        'checkdoi':row['checkdoi'],
        'jaccard_score_title':row['jaccard_score_title'],
        'title_score_85':row['title_score_85'],
        'jaccard_score_journal':row['jaccard_score_journal'],
        'journal_score_85':row['journal_score_85'],
        'pagescore':row['pagescore'],
        'volumscore':row['volumscore']
        }
        val = (int(str(ref_id_index)), json.dumps(val_dict), row['autscore'], row['yearscore'], row['checkdoi'],
               row['jaccard_score_title'], row['title_score_85'], row['jaccard_score_journal'], row['journal_score_85'],
               row['pagescore'], row['volumscore'])
        #list_of_result_tuples.append(val)

        cursor.execute('INSERT INTO match_results_ha(ref_id, match_dict,autscore,yearscore, checkdoi,'
                       'jaccard_score_title ,jaccard_title_85, jaccard_score_journal, jaccard_journal_85, '
                       'pagescore, volumescore) VALUES '
                       '(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', val)

    conn.commit()
    conn.close()


def normauthors_alg(item):
    normalizedtitle = "".join(l for l in item if l not in string.punctuation)
    normalizedtitle = normalizedtitle.lower()
    normalizedtitle = normalizedtitle.replace(" ", "")

    normalizedtitle=normalizedtitle.replace(u'ü', '')
    normalizedtitle=normalizedtitle.replace(u'ä', '')
    normalizedtitle=normalizedtitle.replace(u'ö', '')
    normalizedtitle=normalizedtitle.replace(u'ß', 'ss')
    normalizedtitle =''.join([i if ord(i) < 128 else '' for i in normalizedtitle])
    return normalizedtitle


def Author_title(title,author):
    normalizedtitle = "".join(l for l in title if l not in string.punctuation)
    normalizedtitle = normalizedtitle.lower()
    normalizedtitle = normalizedtitle.replace(" ", "")
    normalizedtitle=normalizedtitle.replace(u'ü', 'u')
    normalizedtitle=normalizedtitle.replace(u'ä', 'a')
    normalizedtitle=normalizedtitle.replace(u'ö', 'o')
    normalizedtitle=normalizedtitle.replace(u'ß', 'ss')
    normalizedtitle =''.join([i if ord(i) < 128 else '' for i in normalizedtitle])

    if isinstance(author, list):
        new_autors = ','.join(l for l in author)
    else:
        new_autors = author
    norm_authors=[]
    for item in new_autors.split(','):
        norm_authors.append(normauthors_alg(item.strip()))
    return normalizedtitle, norm_authors


def normalize_input(title,author):
        temprec = {}
        ntitles, norm_authors = Author_title(title,author)
        temprec["norm_title_str"] = ntitles
        temprec["author_name"] = norm_authors
        return temprec


def ref_hash_genrater(data1,item_ref):
    COEFFS = data1.as_matrix()
    COEFFS = COEFFS.tolist()
    lsref_hash = []
    minhash_list_ref_title = get_min_hash(item_ref['norm_title_str'], COEFFS, 3)
    item_ref['hash_values'] = minhash_list_ref_title
    return item_ref


def str_to_nr(text):
    return binascii.crc32(bytes(text, 'utf-8') & 0xffffffff)


def get_min_hash(text, coeffs, n=3):
    shingles = get_shingles(text, n)
    lsr=[]
    for coeff in coeffs:
        lsr.append(hash_int(shingles, coeff))
    return lsr


def get_shingles(text, n):
    """
    Get integer representation of each shingle of a
    text string
    @param text:    The text from which you want to get the represenation
    @param n:       The n for ngrams zou want to use
    """
    text = "" if type(text) is float else text
    return list({str_to_nr(text[max(0, p):p+n]) for
                 p in range(1-n, len(text))})


def hash_int(shingles, coeff):
    temresult=[]
    temresult.append(next_prime + 1)
    for s in shingles:
        cal=(coeff[0] * s + coeff[1]) % next_prime
        temresult.append(int(cal))
    return min(temresult)


def generate_coeffs(num_hashes=100):
    """
    Generate the coefficients for the hash functions
    @param num_hashes:  The number of coeffiecient you want to generate
                        (k in literature)
    @return A List of tuples of two randomly generated coefficients
            with the size num_hashes
            Example: [(302934, 2389881),(234209, 938990)]
    """
    coeff_a = list()
    while len(coeff_a) < 2*(num_hashes+1):
        coeff_a.append(random.randint(0, max_shingle_id))
    tem_lent=int(len(coeff_a) / 2)
    coeff_a_1 = coeff_a[:tem_lent]
    coeff_a_2 = coeff_a[tem_lent:]
    coeffs = [c for c in zip(coeff_a_1, coeff_a_2)]
    return coeffs


def compare_min_hash(one, two):
    """
    Comparing two hashes
    @return the approximated jaccard distance generated from two hashes
    """
    nr_same = sum([1 for nr in range(len(one)) if
                  one[nr] == two[nr]])

    return float(float(nr_same)/len(one))


def Crossrefdoiextractor(text):
        dictcross=json.loads(text)
        if len(dictcross)>0:
            if "DOI" in dictcross.keys():
                return dictcross["DOI"]
            else:
                return np.nan
        else:
            return np.nan


def str_to_nr(text):
    return binascii.crc32(bytes(text, "utf-8")) & 0xffffffff


def get_shingles(text, n):
    """
    Get integer representation of each shingle of a
    text string
    @param text:    The text from which you want to get the represenation
    @param n:       The n for ngrams zou want to use
    """
    text = "" if type(text) is float else text
    return list({str_to_nr(text[max(0, p):p+n]) for
                 p in range(1-n, len(text))})


def DistJaccard(list1, list2):
    str1 = set(list1)
    str2 = set(list2)
    return float(len(str1 & str2)) / len(str1 | str2)


def author_crossref(text):
        ls_aut=[]
        dictcross=json.loads(text)
        if len(dictcross)>0:
            if "author" in dictcross.keys():
                for item in dictcross["author"]:
                    if 'family' in item.keys():
                        ls_aut.append(item['family'])
            else:
                return np.nan
        else:
            return np.nan
        strtext=""
        if len(ls_aut):
            for item in ls_aut:
                if strtext=="":
                    pass
                else:
                    strtext=strtext+","
                strtext=strtext+item
        else:
            return np.nan
        testtitle=""
        testauthor=strtext
        Ref_norm=normalize_input(testtitle,testauthor)
        return Ref_norm['author_name']


def journal_crossref(text):
    ls_title = []
    dictcross = json.loads(text)
    if len(dictcross) > 0:
        if "container-title" in dictcross.keys():
            for item in dictcross["container-title"]:
                ls_title.append(item)
        else:
            return np.nan
    else:
        return np.nan
    ls_normtitle = []
    for item in ls_title:
        testtitle = item
        testauthor = ""
        Ref_norm = normalize_input(testtitle, testauthor)
        ls_normtitle.append(Ref_norm['norm_title_str'])
    return ls_normtitle


def norm_journal(df):
    testtitle=df
    testauthor=""
    ls_normjournal=[]
    for title in testtitle:
        t, a = Author_title(title,testauthor)
        #Ref_norm=normalize_input(testtitle,testauthor)
        #Exctie_journal=Ref_norm["norm_title_str"]
        ls_normjournal.append(t)
    result = "".join(ls_normjournal)
    if len(result.strip()) > 0:
        return result
    else:
        return np.nan


def jaccard_journal(df):
    import ast
    list_titlecrossref=df["crossref_journal"]
    Exctie_journal=df["journals_norm"]
    if len(Exctie_journal)<1  or len(list_titlecrossref)<1:
        return 0
    else:
        try:
            lsminhashscore=[]
            for item in list_titlecrossref:
                lsminhashscore.append(DistJaccard(get_shingles(Exctie_journal,3),get_shingles(item,3)))

        except Exception as e:
                print (str(e))
                print ("\n")
                traceback.print_exc(file=sys.stdout)
                print("================")
        if len(lsminhashscore) > 0:
            return max(lsminhashscore)
        else: return 0


def title_crossref(text):
        ls_title=[]
        dictcross=json.loads(text)
        if len(dictcross)>0:
            if "title" in dictcross.keys():
                for item in dictcross["title"]:
                        ls_title.append(item)
            else:
                return np.nan
        else:
            return np.nan
        ls_normtitle=[]
        for item in ls_title:
            testtitle=item
            testauthor=""
            Ref_norm=normalize_input(testtitle,testauthor)
            ls_normtitle.append(Ref_norm['norm_title_str'])
        return ls_normtitle


def filter_year(list_date):
    now = datetime.datetime.now()
    currentyear=now.year
    currentyear=int(currentyear)
    ls_year=[]
    for date in list_date:
        for year in date:
            try:
                if int(year)>1200:
                    if int(year)==currentyear or currentyear>int(year):
                        ls_year.append(year)
                else:
                    pass
            except:
                pass
    return ls_year


def crossref_year(text):
        dictcross=json.loads(text)
        if len(dictcross)>0:
            if "issued" in dictcross.keys():
                if 'date-parts' in dictcross["issued"]:
                    lsyearfilter=filter_year(dictcross["issued"]["date-parts"])
                    if len(lsyearfilter)>0:
                        return lsyearfilter
                    else:
                         return np.nan
                else:
                    return np.nan
            else:
                return np.nan
        else:
                return np.nan



def Max_minhash_titles(df):
    list_titlecrossref=df["crossref_title"]
    testtitle=df["title"]
    testauthor=""
    Ref_norm=normalize_input(testtitle,testauthor)
    Exctie_title=Ref_norm["norm_title_str"]
    try:
        lsminhashscore=[]
        ref1 = {}
        ref1["norm_title_str"] = Exctie_title
        ref1=ref_hash_genrater(data1,ref1)
        for item in list_titlecrossref:
            ref2 = {}
            ref2["norm_title_str"] = item
            ref2=ref_hash_genrater(data1,ref2)
            lsminhashscore.append(compare_min_hash(ref2["hash_values"],ref1["hash_values"]))
    except:
            print("======")
            print(list_titlecrossref)
            print(Exctie_title)
            print("================")
    return max(lsminhashscore)



def jaccard_titles(df):
    list_titlecrossref=df["crossref_title"]
    testtitle=df["title"]
    testauthor=""
    Ref_norm=normalize_input(testtitle,testauthor)
    Exctie_title=Ref_norm["norm_title_str"]
    try:
        lsminhashscore=[]
        for item in list_titlecrossref:
            lsminhashscore.append(DistJaccard(get_shingles(Exctie_title,3),get_shingles(item,3)))


    except Exception as e:
            print (str(e))
            print ("\n")
            traceback.print_exc(file=sys.stdout)
            print("================")
    if len(lsminhashscore) > 0:
        return max(lsminhashscore)
    else: return 0


def filteryear2(itemyear):
    year = ''
    try:
        if 1000<int(itemyear) and 3000>int(itemyear):
            year=itemyear
    except:
        pass
    return year


def checkyear(Year_item):
    listofyear=[]
    for itemyis in Year_item:
        for itemyis_s in itemyis.split(' '):
            item_iter_year=filteryear2(itemyis_s)
            if item_iter_year!="":
                listofyear.append(int(item_iter_year))
    return listofyear


def intersectyear(a):
    anb = set(a["crossref_year"]) & set(a["clean_year"])
    if anb:
        return True
    else:
        return False


def intersectaut(a):
    anb = set(a["crossref_author"]) & set(a["norm_aut"])
    if anb:
        return True
    else:
        return False


def intersectvolume_numbers(a):
    anb = set(a["crossref_volume"]) & set(a["volume_numbers"])
    if anb:
        return True
    else:
        return False


def intersectpages(a):
    anb = set(a["crossref_page"]) & set(a["pages"])
    if anb:
        return True
    else:
        return False


def normalise_pages(page):
    results = [int(i) for i in re.findall(r'\d+', str(page))]
    return results


def normalise_crossref_pages(text):
    dictcross = json.loads(text)
    pages = []
    if len(dictcross) > 0:
        if "page" in dictcross.keys():
            page_str = dictcross["page"]
            pages = [int(i) for i in re.findall(r'\d+', str(page_str))]
    return pages


def normalise_crossref_volume(text):
    dictcross = json.loads(text)
    vol = []
    if len(dictcross) > 0:
        if "volume" in dictcross.keys():
            vol_str = dictcross["volume"]
            vol = [int(i) for i in re.findall(r'\d+', str(vol_str))]
    return vol


def compare_page_numbers(t_pages, cr_pages):
    return len(set(t_pages).intersection(cr_pages)) > 0


def extracted_aut(text):
    testtitle=""
    testauthor=text
    Ref_norm=normalize_input(testtitle,testauthor)
    return Ref_norm["author_name"]


def calculate_similarity(psql_login_data, chunk_size=25000):
    try:
        conn = psycopg2.connect(dbname=psql_login_data[0], user=psql_login_data[1], host=psql_login_data[2], password=psql_login_data[3])
    except psycopg2.Error as e:
        print("Unable to connect!")
        print(e)
        sys.exit(1)

    bibtex_fetch = 0
    crossref_fetch = 0
    offset = 0
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS similarity_matches_ha;")
    cursor.execute("CREATE TABLE IF NOT EXISTS similarity_matches_ha "
                   "("
                   "ref_id INTEGER NOT NULL, "
                   "query_doi TEXT, "
                   "query_author TEXT, "
                   "query_title TEXT, "
                   "query_year TEXT "
                   ");")
    conn.commit()
    cursor.execute("DROP TABLE IF EXISTS match_results_ha;")
    cursor.execute("CREATE TABLE IF NOT EXISTS match_results_ha "
                   "("
                   "ref_id INTEGER PRIMARY KEY NOT NULL, "
                   "match_dict TEXT, "
                   "autscore BOOLEAN, "
                   "yearscore BOOLEAN, "
                   "checkdoi BOOLEAN, "
                   "jaccard_score_title NUMERIC , "
                   "jaccard_title_85 BOOLEAN , "
                   "jaccard_score_journal NUMERIC , "
                   "jaccard_journal_85 BOOLEAN , "
                   "pagescore BOOLEAN,"
                   "volumescore BOOLEAN "
                   ");")
    conn.commit()
    cursor.execute("SELECT COUNT(*) FROM crossref_references_ha;")

    total_length = cursor.fetchone()[0]
    #total_length = 100000
    cnt = 0
    while True:
        cnt +=1
        # cancel execution: end of table reached
        if offset >= total_length:
            break

        # --- read dataframes ---
        sql = "SELECT ref_id, crossref_value_json FROM crossref_references_ha ORDER BY ref_id ASC OFFSET %d LIMIT %d " % (offset, chunk_size)
        crossref_data = pd_psql.read_sql(sql, conn)
        crossref_fetch += len(crossref_data)
        print("offset old: ", offset, "offset new: ", str(offset+chunk_size), " total length: ", total_length)

        offset += chunk_size

        # transform ref_id column of the match_info dataframe into a python list
        ref_id_numpy = crossref_data['ref_id'].tolist()
        ref_id_list = [int(str(x)) for x in ref_id_numpy]

        # get all crossref values, where ref_id matches the ref_id of the 'match_info' dataframe
        sql = "select * from join_ref_bibtex_match_ha where ref_id in %s"
        match_info = pd.read_sql(sql, conn, params=[tuple(ref_id_list)])
        bibtex_fetch += len(match_info)
        # resign index columns
        crossref_data.set_index("ref_id", inplace=True)
        # match_info =pd.read_csv("bibtex_ha.csv", sep=",")
        match_info.set_index("ref_id", inplace=True)
        # append crossref json to match_info dataframe.
        match_info["crossref"] = np.nan
        match_info["crossref"] = crossref_data["crossref_value_json"]

        if len(match_info) > 0:
            result_df = calcu_sim(match_info)
            save_match_dict_to_db(psql_login_data, result_df)

        match_info = None
        crossref_data = None


def calcu_sim(joined_table):
    #COEFFS = generate_coeffs()
    #df3 = pd.DataFrame(COEFFS)
    #df3.to_csv('list_of_coeeff.csv', index=False)

    crossrefvalue=joined_table[~joined_table["crossref"].isnull()] #9197

    # --- extract DOI  ---

    crossrefvalue["crossrefdoi"]=crossrefvalue["crossref"].apply(Crossrefdoiextractor)

    checkdoi=crossrefvalue[~crossrefvalue["crossrefdoi"].isnull()][~crossrefvalue["doi"].isnull()]

    checkdoi["checkdoi"]=checkdoi["crossrefdoi"]==checkdoi["doi"]

    crossrefvalue["checkdoi"]=checkdoi["checkdoi"]

    # --- extract author  ---

    crossrefvalue["crossref_author"]=crossrefvalue["crossref"].apply(author_crossref)
    crossrefvalue.to_pickle('cr.pkl')

    # --- extract title  ---
    crossrefvalue["crossref_title"]=crossrefvalue["crossref"].apply(title_crossref)

    #  --- extract journal  ---
    crossrefvalue["journals_norm"] = crossrefvalue['journals'].apply(norm_journal)
    crossrefvalue["crossref_journal"] = crossrefvalue["crossref"].apply(journal_crossref)

    # --- extract year  ---

    crossrefvalue["crossref_year"]=crossrefvalue["crossref"].apply(crossref_year)

    crossrefvalue["crossref_len_title"]=crossrefvalue[~crossrefvalue["crossref_title"].isnull()]["crossref_title"].apply(len)

    #data1 = pd.read_csv('list_of_coeeff.csv')

    # --- calculate jaccard ---
    cross_title = crossrefvalue[~crossrefvalue["crossref_title"].isnull()][~crossrefvalue["title"].isnull()]
    crossrefvalue["jaccard_score_title"]=cross_title.apply(jaccard_titles, axis=1)
    crossrefvalue["title_score_85"]=crossrefvalue["jaccard_score_title"]>=0.85

    crossrefvalue["jaccard_score_journal"] = crossrefvalue[~crossrefvalue["crossref_journal"].isnull()][~crossrefvalue["journals_norm"].isnull()].apply(
        jaccard_journal, axis=1)
    crossrefvalue["journal_score_85"] = crossrefvalue["jaccard_score_journal"] >=0.85

    # --- check pages

    crossrefvalue['pages'] =crossrefvalue["pages"].apply(normalise_pages)
    crossrefvalue['volume'] = crossrefvalue['volume'].apply(normalise_pages)
    crossrefvalue['numbers'] = crossrefvalue['numbers'].apply(normalise_pages)
    crossrefvalue['volume_numbers'] = crossrefvalue['volume'] + crossrefvalue['numbers']

    crossrefvalue['crossref_page'] = crossrefvalue["crossref"].apply(normalise_crossref_pages)
    crossrefvalue['crossref_volume'] = crossrefvalue["crossref"].apply(normalise_crossref_volume)

    crossrefvalue["pagescore"] = crossrefvalue.apply(intersectpages, axis=1)
    crossrefvalue["volumscore"] = crossrefvalue.apply(intersectvolume_numbers, axis=1)

    # --- check year ---

    crossrefvalue["clean_year"]=crossrefvalue["year"].apply(checkyear)

    crossrefvalue["yearscore"]=crossrefvalue[~crossrefvalue["clean_year"].isnull()][~crossrefvalue["crossref_year"].isnull()].apply(intersectyear, axis=1)

    crossrefvalue["norm_aut"]=crossrefvalue["author_name"].apply(extracted_aut)

    crossrefvalue["autscore"]=crossrefvalue[~crossrefvalue["norm_aut"].isnull()][~crossrefvalue["crossref_author"].isnull()].apply(intersectaut, axis=1)

    crossrefvalue["autscore"]=crossrefvalue["autscore"].fillna(False)
    crossrefvalue["yearscore"]=crossrefvalue["yearscore"].fillna(False)
    crossrefvalue["title_score_85"]=crossrefvalue["title_score_85"].fillna(False)
    crossrefvalue["final_score_yta"]= (crossrefvalue["autscore"] &  crossrefvalue["title_score_85"]) | (crossrefvalue["yearscore"] &  crossrefvalue["title_score_85"])
    crossrefvalue["checkdoi"]=crossrefvalue["checkdoi"].fillna(False)

    crossrefvalue["match_id"]=crossrefvalue["match_id"].fillna(False)
    crossrefvalue["jaccard_score_journal"] = crossrefvalue["jaccard_score_journal"].fillna(0)
    crossrefvalue["jaccard_score_title"] = crossrefvalue["jaccard_score_title"].fillna(0)

    # --- shrink dataframe
    shrinked_crossvalue = crossrefvalue[['autscore', 'yearscore',
                                         'checkdoi', 'jaccard_score_title','title_score_85',
                                         'jaccard_score_journal','journal_score_85',
                                         'pagescore', 'volumscore']]

    return shrinked_crossvalue

"""
if __name__ == '__main__':
    psql_login_data = ["db", "user", "host", "pass"]
    calculate_similarity(None, 50000)
"""
