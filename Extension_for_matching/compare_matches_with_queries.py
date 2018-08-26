import pandas as pd
import psycopg2
import sys
import pandas.io.sql as pd_psql
import pickle
import numpy as np


def convert_to_list(s):
    return list(filter(None, "".join(s.split()).strip('{').strip('}').split(',')))

# read queries table
def get__queries_from_psql(psql_login_data):
    try:
        conn = psycopg2.connect(dbname=psql_login_data[0], user=psql_login_data[1], host=psql_login_data[2],
                                password=psql_login_data[3])
    except psycopg2.Error as e:
        print("Unable to connect!")
        print(e)
        sys.exit(1)

    sql = "SELECT * FROM query_precision_ha"
    query_data = pd_psql.read_sql(sql, conn)
    conn.close()
    return query_data


def compare_match_with_query(match, query_df):
    # maps column names of table query_precision_ha to column names of table  match_results_ha
    column_mapping = {'author': 'autscore', 'year': 'yearscore', 'doi': 'checkdoi',
                      'title': 'jaccard_title_85', 'journal': 'jaccard_journal_85', 'pages': 'pagescore',
                      'number': 'volumescore'}

    possiblities = ['author', 'doi', 'journal', 'number', 'pages', 'title', 'year']
    for ind, row in query_df.iterrows():
        query = row['query']
        max_positives = len(query)
        number_of_positives = 0
        for p in query:
            match_col_name = column_mapping[p]
            if match[str(match_col_name)] == True:
                number_of_positives += 1

        if number_of_positives == max_positives:
            return query
    return np.nan


def save_to_db(cursor, result_df):
    for ref_id_index, row in result_df.iterrows():
        val = (row['ref_id'],  row['matched_query'])
        cursor.execute('INSERT INTO query_matches_compare_results_ha (ref_id, query) VALUES '
                       '(%s, %s)', val)


def match_query_with_results(psql_login_data, chunk_size):
    # get queries
    queries = get__queries_from_psql(psql_login_data)
    queries['query'] = queries['query'].apply(convert_to_list)

    try:
        conn = psycopg2.connect(dbname=psql_login_data[0], user=psql_login_data[1], host=psql_login_data[2], password=psql_login_data[3])
    except psycopg2.Error as e:
        print("Unable to connect!")
        print(e)
        sys.exit(1)
    cursor = conn.cursor()

    # drop old results and generate new table
    cursor.execute("DROP TABLE IF EXISTS query_matches_compare_results_ha;")
    cursor.execute("CREATE TABLE IF NOT EXISTS query_matches_compare_results_ha "
                   "("
                   "ref_id INTEGER PRIMARY KEY, "
                   "query TEXT "
                   ");")
    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM match_results_ha;")
    total_length = cursor.fetchone()[0]
    offset = 0
    matches_fetch = 0

    while True:
        # cancel execution: end of table reached
        if offset >= total_length:
            break

        # --- read dataframe ---
        sql = "SELECT * FROM match_results_ha ORDER BY ref_id ASC OFFSET %d LIMIT %d " % (offset, chunk_size)
        match_data = pd_psql.read_sql(sql, conn)
        matches_fetch += len(match_data)
        #print("fetched: ", matches_fetch, " offset old: ", offset, "offset new: ", str(offset+chunk_size), " total length: ", total_length)

        offset += chunk_size

        if len(match_data) > 0:
            match_data['matched_query'] = match_data.apply(compare_match_with_query, args=(queries,), axis=1)
            save_to_db(cursor, match_data[~match_data['matched_query'].isnull()])
            conn.commit()
        match_data = None
    conn.close()
