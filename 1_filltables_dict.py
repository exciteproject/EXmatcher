import json
from matcher_query.main_matcher import result_for_match
import psycopg2
from tqdm import tqdm
from aux_function import *
import pandas as pd
from multiprocessing import Pool

configfile = json.load(open('./configfile.json'))

def datageneratorforqueries(x):
    ls = []
    try:
        data1 = (x[1][0], json.dumps(dict_generator(json.loads(x[1][3]))))
        sowi_result_records = result_for_match(dict_generator(json.loads(x[1][3])),x[1][2])
        for item1 in sowi_result_records:
            data = (x[1][0], json.dumps(item1))
            ls.append(data)
        return [data1, ls]

    except Exception as e:
        print(e)


def mulit_process_generatdata_for_query(dfselect_row):
    p = Pool(4)
    return list(tqdm(p.imap(datageneratorforqueries, dfselect_row.iterrows())))


def dicttotuplesowiport_psql(ref_id, keys_flag, item3):
    positioninlist = list(item3.keys())[0]
    item3 = item3[positioninlist]
    return (ref_id, keys_flag, json.dumps(item3), item3.get("id", "nv"),
            item3.get("title_full", "nv"), item3.get("title_sub", "nv"), item3.get("facet_person_str_mv", "nv"),
            item3.get("person_author_normalized_str_mv", "nv"), item3.get("norm_title_str", "nv"),
            item3.get("norm_title_full_str", "nv"), item3.get("norm_publishDate_str", "nv"),
            item3.get("norm_pagerange_str", "nv"), item3.get("recorddoi_str_mv", "nv"),
            item3.get("norm_volume_str", "nv"), item3.get("norm_issue_str", "nv"),
            item3.get("journal_title_txt_mv", "nv"),
            item3.get("journal_short_txt_mv", "nv"), positioninlist)

            
            
def fill_tables_for_refiddict_refid_match():            
    conn = psycopg2.connect("dbname="+configfile['dbname']+" user="+configfile['user']+" host="+configfile['host']+" password="+configfile['password'])
    cur = conn.cursor()
    query = """select * from BNA_source_table_match_test"""
    cur.execute(query)
    rows = cur.fetchall()
    dfselect_row = pd.DataFrame(rows)

    listofresult = mulit_process_generatdata_for_query(dfselect_row)

    conn = psycopg2.connect("dbname="+configfile['dbname']+" user="+configfile['user']+" host="+configfile['host']+" password="+configfile['password'])
    cur = conn.cursor()
    for item in tqdm(listofresult):
        try:
            query = """INSERT INTO BNA_dict_table_match_test(ref_id, Dict_for_matching) VALUES (%s, %s)"""
            cur.execute(query, item[0])
        except:
            print(item[0])
            print("error")
            break
    conn.commit()

    cur = conn.cursor()
    query = """INSERT INTO bna_records_table_sowimatch_test
        (ref_id,keys_flag,resultfromsowi,sowi_id,
        sowi_title_full,sowi_title_sub,sowi_facet_person_str_mv,sowi_person_author_normalized_str_mv,
        sowi_norm_title_str,sowi_norm_title_full_str,soiw_norm_publishdate_str,sowi_norm_pagerange_str,
        sowi_recorddoi_str_mv,sowi_norm_volume_str,sowi_norm_issue_str,sowi_journal_title_txt_mv,
        sowi_journal_short_txt_mv,position_in_list) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                                            %s, %s, %s, %s, %s, %s)"""

    for item in listofresult:
        for item1 in item[1]:
            item11 = json.loads(item1[1])
            mat_records = item11["match_id"]
            if str(mat_records) != "not_match":
                if len(mat_records) > 0:
                    for item3 in mat_records:
                        cur.execute(query, dicttotuplesowiport_psql(item1[0], item11["keys_flag"], item3))

    conn.commit()
    
    
fill_tables_for_refiddict_refid_match()
