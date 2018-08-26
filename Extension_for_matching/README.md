# Crossref_extension_for_matching

This repository is created to match extracted references, which are not matched with the [main-approach](https://github.com/exciteproject/EXMATCHER), using the Crossref API.

This procedure consists of four steps. Each step is implemented in a separate Python module.
1. Join records from segments of references from multiple PSQL tables into one, call Crossref API and save results to PSQL. 
2. Compare the similarity of Crossref results with the references from the first step. 
3. Extract key fields (e.g. author, author + title, title + journal) from a csv, which achieved a certain precision/recall/f-measure. 
4. Compare results from (2.) with key fields from (3.). If the features of record from the second step match a list of key fields, save the record and the key fields it matched in a PSQL table.

-----
### First module: crossref_api_not_match.py
Prepares PSQL data containg non-matched sowiport records, calls Crossref API for each record and save top result in a PSQL table.

#### Usage:
First edit line 105 in crossref_api_not_match.py and insert a valid e-mail address for the Crossref API. You also need login credentials for the  Postgres database and put it in a list as shown below. After that, the code can be used:
```
import crossref_api_not_match crm

# login credentials
psql_login_data = ["database", "username", "host", "password"]

# Joins multiple PSQL tables on ref_id
crm.join_tables(psql_login_data)

# reads from joined table, in chunks, calls Crossref API with each chunk and saves Crossref results as JSON to PSQL. Chunk size (second parameter) can be adjusted, if desired.
crm.crossref_via_database(psql_login_data, 250)
```

-----
### Second module: calculate_crossref_bibtex_similarity.py
Calculates the similarity of the Crossref API records from the first module with their corresponding input records. Compares similarity of: Author, title, DOI, Year, Journal, Pages, Volume. Results are saved in a PSQL table with boolean value, if a field is a match or not.


#### Usage
Import module and call function _calculate_similarity()_. Function compares similarity for n records from database.
First parameter is a list with PSQL login credentials.
second parameter defines chunk size. Default = 25000.   
```
import calculate_crossref_bibtex_similarity as crsim

# login credentials
psql_login_data = ["database", "username", "host", "password"]
crsim.calculate_similarity(psql_login_data, 50000)
"""
```

-----
### Third module: shrink_query_table.py
This module reads a csv file containing recall and precision for a combination of queries, calculates f-measure and shrinks table by chosen measurement and threshold. Results are saved in a PSQL table. After shrinking, duplicate queries, e.g. queries which are already part of a larger query, are removed.   

Snippet of csv:

| query        | precision           | recall  |
| ------------- |:-------------:| -----:|
| (number, title, year)      | 1 | 0.840708 |
| (author, number, title, year)      | 1      |   0.819425 |
| (pages, title) | 1      |    0.695013 |


#### Usage
Import module and pandas. Read csv into Pandas dataframe and call *shrink_table()*
as shown below.

***shrink_table()*** parameters:

*First*:  pandas dataframe containing csv data.

*Second*: PSQL login data

*Third*: Chose measurement. 'p' = Precision, 'r' = Recall, 'f' = F-measure.

*Fourth*: Threshold between 0 - 1.0
```
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
Compares the results of similarity between sowiport and crossref references, from the second module, with the retrieved queries of the third module. For each reference, the module checks if there is at least one query, where all fields of the reference are true for each field in the query. The comparison runs in chunks, whose size can be set as a parameter. Results are saved in a PSQL table containing *ref_id* of the reference and what query it matched.   


#### Usage
Import and run code as shown below.  

*First* parameter of the function is the PSQL login data. *SEcond* parameter is the number of records in a chunk.
```
import compare_matches_with_queries as cmpq

# psql login data
psql_login_data = ["database", "username", "host", "password"]
# first parameter
cmpq.match_query_with_results(psql_login_data, 25000)
```
