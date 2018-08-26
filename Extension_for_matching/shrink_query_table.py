import pandas as pd
import psycopg2
import sys
import pandas.io.sql as pd_psql
import pickle
import numpy as np

"""
author: Haydar Akyürek

description:
Python 3 script to retrieve best matching queries.
"""

def get_query_by_threshold(dataframe, measure, value):
    dataframe.rename(columns={dataframe.columns[0]: 'query'}, inplace=True)
    col_name = ''
    if measure == 'r':
        col_name = 'avg_recall'
    elif measure == 'p':
        col_name = 'avg_precision'
    elif measure == 'f':
        col_name = 'fmeasure'

    dataframe["fmeasure"] = 2 * (dataframe["avg_recall"] * dataframe["avg_precision"]) / (
            dataframe["avg_recall"] + dataframe["avg_precision"])
    results = dataframe.loc[dataframe[col_name] >= float(value)]
    results.sort_values(['avg_precision', "fmeasure"], ascending=False, inplace=True)
    return results[['query', 'avg_recall', 'avg_precision', 'fmeasure']]


def generate_sets(tuple_str):
    return list(filter(None, "".join(tuple_str.split()).strip('(').strip(')').split(',')))


def intersection(lst1, lst2):
    return len((set(lst1) & set(lst2)))


def select_duplicates(df):
    for index, row in df.iterrows():
        value = row['query']
        if row['duplicate'] == False:
            for ind, row_two in df.iterrows():
                if index != ind:
                    comp_value = row_two['query']
                    if intersection(comp_value, value) == len(value):
                        df.at[ind, 'duplicate'] = True


def shrink_table(data, psql_login_data,  measure='p', threshold = 0.8):
    #data = pd.read_csv('precision_00_dict_main.csv', encoding='utf-8')
    df = get_query_by_threshold(data, measure, threshold)
    df['query'] = df['query'].apply(generate_sets)
    df['duplicate'] = False
    df.index = df['query'].str.len()
    df = df.sort_index(ascending=True).reset_index(drop=True)
    select_duplicates(df)
    df = df[df["duplicate"] == False]
    df = get_query_by_threshold(df, measure, threshold)
    save_to_db(None, df)
    return df


def save_to_db(psql_login_data, result_df):
    try:
        conn = psycopg2.connect(dbname=psql_login_data[0], user=psql_login_data[1], host=psql_login_data[2], password=psql_login_data[3])
    except psycopg2.Error as e:
        print ("Unable to connect!")
        print (e)
        sys.exit(1)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS query_precision_ha;")
    cursor.execute("CREATE TABLE IF NOT EXISTS query_precision_ha "
                   "("
                   "query TEXT, "
                   "avg_recall NUMERIC , "
                   "avg_precision NUMERIC , "
                   "fmeasure NUMERIC "
                   ");")
    conn.commit()

    for ref_id_index, row in result_df.iterrows():
        val = (row['query'],  row['avg_recall'], row['avg_precision'], row['fmeasure'])
        cursor.execute('INSERT INTO query_precision_ha(query, avg_recall, avg_precision, fmeasure) VALUES '
                       '(%s, %s, %s, %s)', val)

    conn.commit()
    conn.close()
