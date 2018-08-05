import psycopg2
import pandas as pd
import json

configfile = json.load(open('./configfile.json'))

def load_basic_data_into_psqltable():
    df1 = pd.read_csv(configfile['goldstandarddata'])
    input_data = df1
    input_data = input_data[input_data["ref_seg_dic"] != "not well-formed (invalid token): line 1, column 974"]
    conn = psycopg2.connect("dbname="+configfile['dbname']+" user="+configfile['user']+" host="+configfile['host']+" password="+configfile['password'])
    cur = conn.cursor()
    for index, item in input_data[["new_id", "sourcefile", "ref_text_x", "ref_seg_dic"]].iterrows():
        query = """INSERT INTO BNA_source_table_match_test(ref_id, sourcefile , ref_text , segcit_dict) VALUES (%s, %s, %s, %s)"""
        data = (item["new_id"], item["sourcefile"], item["ref_text_x"], item["ref_seg_dic"])
        cur.execute(query, data)
    conn.commit()
    
    
load_basic_data_into_psqltable()
