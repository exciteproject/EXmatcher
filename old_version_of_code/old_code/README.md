# Reference strings matcher

In the repository, you can find different strategies to match reference strings with corresponding 
items in publication databases. Till now two different strategies are implemented:
* Matching with SOLR
* Matching with Minhash

![picture alt](https://s27.postimg.org/65kwnvhwj/stex.png "Matching workflow")

## Algorithm based on SOLR
* Sowiport (http://sowiport.de/) is one of the repositories which will be used as the target of our matching reference strings.
* Sowiport uses Solr for search function and indexing.
* We easily can pass a query to solr like the below query and then receive lists of items:
	<br />(title:aspirin'\~'0.5) AND (author:lewis OR author:blume) AND (Year:1983)
	<br />The tilda ('\~') represents 'Levenshtein-distance'.
	
## Algorithm based on Minhash
1. Generate min hash values for titles in Sowiport (once).
2. Generate min hash values for extracted titles of reference string.
3. Filter items in sowiport by extracted year and authors’ names of reference string.
4. Apply Jaccard comparison on min hash values of titles of reference string and filtered items in sowiport.
5. Rank the sowiport items regarding their Jaccard score.
6. The match item to the reference string is the top item in the ranking which is higher than our predefined threshold.

<br />How to generate 'Min-hash' value (We describe it in the following example):
<br />![picture alt](https://s27.postimg.org/7ns6q7j4z/minhashval.png "Min-hash value")

## Run algorithms:
### SOLR:
1. Put extracted meta data from a reference string in a dictionary like:

		- input_dict={}
		- input_dict['ID']= 7769
		- input_dict['title']= ["BABBIE"," E.R."]
		- input_dict['year']= [" 1979",2001]
		- input_dict['ENTRYTYPE']="article"
		- input_dict['author']= "The Practice of Social Reserch"
	
2. Run binder.main(input_dict)
3. Output will be a dictionary (as match_id in Sowiport) like:

		- {
		- 'Cit_id': '7769', 
		- 'Sowiport_id': 'gesis-bib-101302'
		- }
(and also a query which has sent to solr for retreiving the result)	
### Minhash:
1. Generate bibtex file for reference strings (you can use this simple code:/Extra_tool/Bibtex_generator/Cermin_bibtex.java)
2. Use '/Min_hash_v1/solr_hasher/minhash_title_multi.py' for making minhash value for titles in SOLR
3. Use '/Min_hash_v1/ref_hasher/Genreate_ref_hashe_title.py' for making minhash value for extracted titles of reference strings
4. Compare reference strings and items in DB with '/Combination_Solr_Minhash/Hash_Comparision.py'
 
