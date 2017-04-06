# Reference strings matcher

In the repository, you can find different strategy to match refernce strings with corosponding 
items in publications database. Till now two different strategies are impelemented:
* Matching with SOLR
* Matching with Minhash

![picture alt](https://s27.postimg.org/65kwnvhwj/stex.png "Matching workflow")

## Algorithm based on SOLR
* Sowiport is one of the repositories of publication which will be used as the target of our matching reference strings.
* Sowiport uses Solr for search function and indexing.
* We easily can pass a query to solr like the below query and then receive lists of items:
	(title:aspirin'\~'0.5) AND (author:lewis OR author:blume) AND (Year:1983)
	The tilda ('\~') represents 'Levenshtein-distance'.


 