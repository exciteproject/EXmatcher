# Crossref_extension_for_matching

This code is for matching non-source references, which could not be matched with the [main-approach](https://github.com/exciteproject/EXMATCHER), using the Crossref API.

This procedure consists of four steps.
1. Join segments of references from multiple PostgreSQL tables into one table, call Crossref API for each reference and save the top result to PostgreSQL.
2. Compare the similarity of each Crossref API result with the corresponding reference from the first step and save results, either as Boolean values or with a similarity score, to PostgreSQL.
3. Extract key field combinations (e.g. author, author + title, title + journal), as features, from a CSV file. The key field combinations are filtered by a given precision/ recall/ f-measure threshold.
4. Match the output of the similarity comparison of the second step, with the extracted feature combinations of the third step. For each record, check if there is at least one feature combination, where all fields of the record are true. Save the id of the record and the feature combination, it matched with, in a PostgreSQL table.

-----
### First module: crossref_api_not_match.py
Joins PostgreSQL tables containing segments of not-matched, non-source references, calls Crossref API for each record and saves the top result in a PostgreSQL table.

#### Usage:
You need login credentials for the PostgreSQL database, as well as a legit E-Mail address for the Crossref API.
```python
import crossref_api_not_match crm

# PSQL login credentials
psql_login_data = ["database", "username", "host", "password"]

# E-Mail address for crossref
e_mail_address= "adress@email.com"

# chunk size
crossref_chunk_size = 250      

# Join multiple PSQL tables on ref_id
crm.join_tables(psql_login_data)

# reads from joined table, in chunks, calls Crossref API with each chunk and saves API results as JSON to PostgreSQL.
crm.crossref_via_database(psql_login_data, e_mail_address, crossref_chunk_size)
```

-----
### Second module: calculate_crossref_bibtex_similarity.py
This module calculates the similarity of the Crossref API results with the corresponding input references, from the first module.</br> Compares similarity of author, title, DOI, year, journal, pages, volumes.</br>Results are saved in a PostgreSQL table all compared fields and the corresponding boolean values (and similarity scores) of the matching.


#### Usage
Import module and call function *calculate_similarity()*.
```python
import calculate_crossref_bibtex_similarity as crsim

# login data - needs to be changed with correct lgoin credentials  
psql_login_data = ["database", "username", "host", "password"]

# chunk size
compare_chunk_size = 50000

# Call function with login credentials. Seocn dparamete defines chunk size.
crsim.calculate_similarity(psql_login_data, compare_chunk_size)
```
-----
### Third module: shrink_query_table.py
This module reads a CSV file, containing recall and precision of different key field combinations, calculates f-measure and extracts key field combinations by recall / precision / f-measure and given threshold. After extracting, duplicate key field combinations, e.g. key field combinations, which are part of a larger combination, are removed.
Results are used as features in the next step and saved in a PostgreSQL table.

Snippet of csv:

| query        | precision           | recall  |
| ------------- |:-------------:| -----:|
| (number, title, year)      | 1 | 0.840708 |
| (author, number, title, year)      | 1      |   0.819425 |
| (pages, title) | 1      |    0.695013 |


#### Usage
Import module and pandas. Read csv into Pandas dataframe and call *shrink_table()*
as shown below.

*First parameter*:    pandas dataframe containing csv data.<br/>
*Second parameter*:   PSQL login data <br/>
*Third parameter*:    Measurement. 'p' = Precision, 'r' = Recall, 'f' = F-measure. <br/>
*Fourth parameter*:   Threshold value between 0 and 1
```python
import shrink_query_table as stbl
import pandas as pd

# login credentials for PSQL
psql_login_data = ["database", "user", "host", "pass"]

# read csv file into pandas dataframe.
csv_data = pd.read_csv('precision_00_dict_main.csv', encoding='utf-8')

# shrink table and save results to PSQL
stbl.shrink_table(csv_data, psql_login_data, 'p', 0.9)
```

-----
### Fourth module: compare_matches_with_queries.py
Compares the output of the second module, with the retrieved features of the third module. For each record, the module checks if there is at least one feature combination, where all fields  of the record are true. The comparison runs in chunks, whose size can be set as a parameter. Results, containing the *id* of the reference and what features it matched, are saved in a PostgreSQL table.   

#### Usage
Run code as shown below.  

```python
import compare_matches_with_queries as cmpq

# psql login data
psql_login_data = ["database", "username", "host", "password"]

# chunk size
chunk_size = 25000

# match records with key field combinations.
cmpq.match_query_with_results(psql_login_data, chunk_size)
```
