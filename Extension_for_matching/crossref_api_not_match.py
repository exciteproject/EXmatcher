import json

import numpy as np
import time
import pandas.io.sql as pd_psql
from habanero import Crossref
import pandas as pd
import sys
import psycopg2
reload(sys)
sys.setdefaultencoding('UTF8')


"""
:parameter login_data: List of psql login information like ["database", "user", "host", "password"]
"""
def join_tables(login_data):
    try:
        conn = psycopg2.connect(dbname=login_data[0], user=login_data[1], host=login_data[2], password=login_data[3])
    except psycopg2.Error as e:
        print "Unable to connect!"
        print e
        sys.exit(1)

    cursor = conn.cursor()

    # drop previous version of join_ref_bibtex_match_ha table
    cursor.execute("DROP TABLE IF EXISTS join_ref_bibtex_match_ha;")
    cursor.execute(
        "CREATE TABLE join_ref_bibtex_match_ha AS \
        (SELECT es_newmodel_bibtexes_full_pars.*, \
        es_newmodel_references.meta_id_source, es_newmodel_references.ssoar_id, es_newmodel_references.ref_text, \
        ref_match_ssoar_final_ben.match_id, ref_match_ssoar_final_ben.key_fields \
        FROM es_newmodel_bibtexes_full_pars INNER JOIN es_newmodel_references ON es_newmodel_bibtexes_full_pars.ref_id = es_newmodel_references.ref_id \
        INNER JOIN ref_match_ssoar_final_ben ON es_newmodel_references.ref_id = ref_match_ssoar_final_ben.ref_id)"
    )

    cursor.close()
    conn.commit()


def decode_utf8(var):
    #return var.decode('utf-8', 'ignore').replace(u'\xa0', u'').strip()
    return var

"""
:parameter login_data: List of psql login information like ["database", "user", "host", "password"]
:parameter chunk_size: number of API calls, before temp results are saved
"""
def crossref_via_database(login_data, chunk_size=1000):
    try:
        conn = psycopg2.connect(dbname=login_data[0], user=login_data[1], host=login_data[2], password=login_data[3])
    except psycopg2.Error as e:
        print "Unable to connect!"
        print e
        sys.exit(1)
    cursor = conn.cursor()

    # drop previous version of join_ref_bibtex_match_ha table
    cursor.execute("DROP TABLE IF EXISTS crossref_references_ha;")
    cursor.execute("CREATE TABLE IF NOT EXISTS crossref_references_ha "
                   "("
                   "ref_id INTEGER NOT NULL, "
                   "crossref_value_json TEXT, "
                   "doi TEXT, "
                   "url TEXT,"
                   "issued TEXT, "
                   "volume TEXT, "
                   "issn TEXT, "
                   "short_container_title TEXT,"
                   "publisher TEXT,"
                   "title TEXT,"
                   "author TEXT,"
                   "score TEXT,"
                   "container_title TEXT,"
                   "page TEXT"
                   ");")
    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM join_ref_bibtex_match_ha;")
    offset = 0
    total_length = cursor.fetchone()[0]
    while True:
        # cancel execution: end of table reached
        if offset >= total_length:
            break

        # --- read dataframes ---
        sql = "SELECT ref_id, match_id, ref_text FROM join_ref_bibtex_match_ha ORDER BY ref_id ASC OFFSET %d LIMIT %d " % (
        offset, chunk_size)
        sql_dataframe = pd_psql.read_sql(sql, conn)
        print("offset old: ", offset, "offset new: ", str(offset + chunk_size), " total length: ", total_length)
        offset += chunk_size
        crossref_api(login_data, sql_dataframe, chunk_size)


# method called from crossref_via_database().
# iterates over dataframe, calls Crossref-API with value in [ref_text] column.
# stores result to psql
def crossref_api(login_data, sql_dataframe, chunk_size):
    pd.options.mode.chained_assignment = None
    match_info = sql_dataframe
    sampledata = match_info[(match_info["match_id"]=="not_match") | (match_info["match_id"]=="error")]
    sampledata["crossref"] = np.nan
    cr = Crossref(mailto = "name@email.org")
    i=0

    while True:
        ns = i+chunk_size
        if i < len(sampledata):
            dict_cross = []

            for index, row in sampledata[i:ns].iterrows():
                tempdata = []
                reftext = sampledata.ix[index]["ref_text"]
                try:
                    x = cr.works(query =reftext, limit=1, select ="DOI,title,issued,short-container-title,ISSN,score,URL,title,page,publisher,container-title,DOI,author,volume,issued")
                    tempdata.append(row[0])
                    tempdata.append(x["message"]["items"])
                except:
                    tempdata.append(row[0])
                    tempdata.append(np.nan)
                    print("error"+str(index))
                dict_cross.append(tempdata)
            crossref_to_db(login_data, dict_cross)
            i=i+chunk_size
        else:
            break


# function reads csv files, parses and splits the JSON-like values and inserts the data to a postgresql db.
# Parameters: list, which contains the command-line arguments passed to the script. Arguments should be the login credentials for DB access.
def crossref_to_db(args, source_df):
    cnt = 0  # count data
    invalid_rows = 0  # count non-parsable rows

    try:
        conn = psycopg2.connect(dbname=args[0], user=args[1], host=args[2], password=args[3])
    except psycopg2.Error as e:
        print "Unable to connect!"
        print e
        sys.exit(1)

    cursor = conn.cursor()


    # iterate over files, prase second column with ast.literal_eval() to dict.
    # Search for keys in dict, extract values,
    # decode to unicode, ignore non-utf8 characters and no-breaking spaces (see decode_utf8())
    # save values as a tuple in a list, insert tuples to postgres db.

    result_list_of_tuples = []
    for values in source_df:
        """
        pseudo_json = ""
        try:
            if len(values[1]) > 2:
                pseudo_json = ast.literal_eval(values[1])[0]
        except:
            invalid_rows += 1
            # print values[0], values[1]
            continue
        """
        authors = []
        pseudo_json = values[1][0]
        publisher = pseudo_json['publisher'] if 'publisher' in pseudo_json else ""

        doi = pseudo_json['DOI'] if 'DOI' in pseudo_json else ""

        authors_value = pseudo_json['author'] if 'author' in pseudo_json else ""

        author_str = authors_value
        author_str = json.dumps(author_str, ensure_ascii=False).encode('utf-8')
        volume = pseudo_json['volume'] if 'volume' in pseudo_json else ""

        issn = pseudo_json['issn'] if 'issn' in pseudo_json else ""

        title = pseudo_json['title'] if 'title' in pseudo_json else ""
        title_str = " ".join(str(x) for x in title)

        container_title = pseudo_json['container-title'] if 'container-title' in pseudo_json else ""
        container_title_str = " ".join(str(x) for x in container_title)

        page = pseudo_json['page'] if 'page' in pseudo_json else ""

        url = pseudo_json['url'] if 'url' in pseudo_json else ""

        score = str(pseudo_json['score']) if 'score' in pseudo_json else ""

        short_container_title = pseudo_json[
            'short-container-title'] if 'short_container_title' in pseudo_json else ""
        short_container_title_str = " ".join(str(x) for x in short_container_title)

        issued = str(pseudo_json['issued']) if 'issued' in pseudo_json else ""

        enc_values = json.dumps(pseudo_json, ensure_ascii=False).encode('utf-8')
        result_list_of_tuples.append(
            (values[0], enc_values,
             doi.encode('utf-8'), url.encode('utf-8'), issued.encode('utf-8'), volume.encode('utf-8'), issn.encode('utf-8'), short_container_title_str.encode('utf-8'),
             publisher.encode('utf-8'), title_str.encode('utf-8'), author_str.encode('utf-8'), score.encode('utf-8'), container_title_str.encode('utf-8'), page.encode('utf-8')))

    # classic insert statements for every tuple in the result list.
    for e in result_list_of_tuples:
        cursor.execute('INSERT INTO crossref_references_ha '
                       '(ref_id, crossref_value_json, doi, url, issued, volume, issn, short_container_title, '
                       'publisher, title, author, score, container_title, page) '
                       'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                       e, )
    result_list_of_tuples = []
    conn.commit()

    cursor.close()
