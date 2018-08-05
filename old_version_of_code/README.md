# Reference_Matcher

## General

This algorithm is implemented for finding corresponding items in a bibliography corpus (such as [Sowiport.org](http://sowiport.gesis.org/) or 
[related-work.net](http://related-work.net/#colorful)) for reference strings. 
In [EXCITE](https://west.uni-koblenz.de/en/research/excite)
 project the reference strings are extracted via [RefExt](https://github.com/exciteproject/refext)
 , and afterward, they are processed with a metadata extraction to find out 
their metadata (such as author, title, year, page, etc.). These metadata are used in the reference matcher algorithm.


The [matcher algorithm](https://github.com/exciteproject/ref_matcher) depends on [SOLR](http://lucene.apache.org/solr/)
 platform. By investigating  the references of [SSOAR](http://www.ssoar.info/)
 corpus, we noticed that seven bibliographic fields are most commonly used 
**[“author”, “title”, “year”, “doi”, “page”, “number or volume”, “journal”]**. All possible combination of these fields is **127**. A list of field 
combinations which has higher precision than a threshold is defined based on analyzing our [gold standard](https://github.com/exciteproject/ref_matcher/blob/master/solr_matcher_algorithm/Match_solr_quries/Data/sowiIdDuplicatesEdit.csv)
. This gold standard contains information 
about **600** randomly selected reference strings from SSOAR corpus and their IDs to Sowiport.


## How to use

### Step 1. Make a dictionary based on a precision threshold:

You only need to decide which precision threshold you want to have. The [module](https://github.com/exciteproject/ref_matcher/blob/master/solr_matcher_algorithm/Match_solr_quries/5_select_candidate_queries.py)
 is responsible for generating a dictionary which each 
of its keys is a possible set of fields and value for this key is all possible combinations of these fields which return a match with precisions higher than the defined 
threshold (a float number in the range between 0 and 1). You need to change the threshold in line 28 in the module into your desired number and then run the following code:



    $ Python 5_select_candidate_queries.py 
    
The result will be a dictionary which is saved in a CSV file. Here is an [example file](https://github.com/exciteproject/ref_matcher/blob/master/matcher_algorithm/precision_09_dict_query.csv)
 for the precisions higher than 0.9.

### Step 2. Find a match for a reference string:

After running the first step, you easily can process a BibTeX string and receive a result of matching for that. The following sample code demonstrates  
how it works:

```python

from main_matcher import result_for_match

bibtex_str='@article{CourgeauD1980,\nauthor = {Courgeau D.},\ntitle = {Analyse quantitative des migrations humaines},\nyear = {1980},\n}\n'

match_id, Key_fields = result_for_match(bibtex_str)

return match_id, Key_fields
```

The algorithm returns two variables. The first one is for representing ID of a corresponding item in target bibliographical database for the input BibTeX string. 
The second variable shows the combination of fields which is used for generating the result. The result for the above code will be:

```python 
    matchid="ubk-opac-LY000636844"
    Key_fields="year,title"
```

The matchid variable referes to the URL: http://sowiport.gesis.org/search/id/ubk-opac-LY000636844

