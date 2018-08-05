import psycopg2
from aux_function import *
import pandas as pd
import json
from tqdm import tqdm

configfile = json.load(open('./configfile.json'))

def remove_dublicate():
    conn = psycopg2.connect("dbname="+configfile['dbname']+" user="+configfile['user']+" host="+configfile['host']+" password="+configfile['password'])
    cur = conn.cursor()
    query = """select ref_id, sowi_id ,min(position_in_list), min(auxnr), count(*) from bna_records_table_sowimatch_test group by ref_id, sowi_id;"""
    cur.execute(query)
    rows = cur.fetchall()

    df67 = pd.DataFrame(rows)
    cur = conn.cursor()
    ls_delete_item = []
    for index, item in tqdm(df67.iterrows()):
        query = """select min(auxnr) FROM bna_records_table_sowimatch_test WHERE ref_id = %s AND sowi_id = %s AND position_in_list = %s;"""
        cur.execute(query, (str(item[0]), str(item[1]), str(item[2], )))
        conn.commit()
        rows = cur.fetchall()
        ls_delete_item.append((str(item[0]), str(item[1]), rows[0][0]))

    for item in tqdm(ls_delete_item):
        query = """Delete FROM bna_records_table_sowimatch_test WHERE ref_id = %s AND sowi_id = %s AND auxnr <> %s;"""
        cur.execute(query, (str(item[0]), str(item[1]), str(item[2], )))
        conn.commit()

        
remove_dublicate()