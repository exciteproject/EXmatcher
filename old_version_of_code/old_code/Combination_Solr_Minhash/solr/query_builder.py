# -*- coding: UTF-8 -*-
import json
import codecs

__author__ = "ottowg"


class SOLRQueryBuilder(object):
    """
    Generates SOLR queries for records on specified fields.
    Is able to generate fuzzy queries.
    Is able to join queries with defined operators.
    """
    def create_query(self,
                     record,
                     source_field,
                     query_fields,
                     operator, **params):
        """
        creates a base query depending on a record.
        The mapping is defined in source_field and query fields.
        It is possible to map one source_field to different
        query_fields joined with a defined operator.
        If there is a list inside the record entry defined by
        source_field each entry of the list will be joined by
        defined operator in the query_field entry.
        @param record:          The record containing the values used for the
                                query
                                Example:
                                {"my_author_field": ["Miller", "Meyer"]}
        @param source_field:    The source_field in the given record
                                which contains the value
                                Example:
                                "my_author_field"
        @param query_fields:    A List of query_field info to define on which
                                solr fields the query should be made
                                Each query_field info has to contain a dict.
                                Example:
                                [{"query_field": "author",
                                 "operator": " AND "
                                 "fuzzy": 0.5},
                                 {"query_field": "author_short",
                                  "operator": " OR "}]
                                operator defines how to join values for this
                                query_field
                                optional "fuzzy" parameter defines a fuzzy
                                factor when needed
        @param operator:        Defines how to join the query_field queries
                                to one.
                                Example:
                                " OR "
        @return:                A valid solr query
                                Example:
                                "(author:Miller~0.5 AND author:Meyer~0.5)
                                    OR
                                 (author_short:Miller OR author_short:Meyer)"
        """
        query_parts = []
        default_inner_operator = " OR "
        if source_field in record:
            value_list = self._clean_values(record[source_field])

            for field_info in query_fields:
                this_value_list = value_list
                if "fuzzy" in field_info:
                    # there are need single quotes with fuzzy
                    this_value_list = ["%s~%.1f" % (v, field_info["fuzzy"])
                                       for v in this_value_list]
                this_value_list = ["%s:%s" % (
                                       field_info["query_field"],
                                       v) for v in this_value_list]
                if "operator" in field_info:
                    part = field_info["operator"].join(this_value_list)
                else:
                    part = default_inner_operator.join(this_value_list)

                if part != "":
                    query_parts.append("(%s)" % part)
        query = operator.join(query_parts)

        return query

    def combine_query(self, list_of_queries, operator=" AND "):
        """
        Combines two queries with a given operator
        """
        list_of_queries = ["(%s)" % q for q in list_of_queries]
        query = operator.join(list_of_queries)

        return query

    def _clean_values(self, value_list):
        """
        Clean the values to prevent empty query fields
        and unescaped ":" in values
        """
        if type(value_list) != list:
            value_list = [value_list]
        value_list = [str(v).replace(":", "\:")
                      for v in value_list if v is not None and
                      str(v).strip() != ""]

        return value_list


class SOLRMappingQueryBuilder(SOLRQueryBuilder):
    """
    Generates queries with given definition in mapping file
    in json or mapping dictionary
    """
    def __init__(self, query_mapping={}):
        """
        @param query_mapping: The query mapping you want to
        use to create the queries
        Could be a file name or a dictionary.
        Contains three parts:
        "add_query"
        Here query mapping could be defined

        "combine_query"
        "queries_to_ask"
        """
        self.document = {}
        if type(query_mapping) == dict:
            self.query_mapping = query_mapping
        else:
            with codecs.open(query_mapping) as mf:
                self.query_mapping = json.load(mf)

    def generate_queries(self, document):
        self.document = document
        self.query_dict = {}

        if "add_query" in self.query_mapping:
            for query_to_add in self.query_mapping["add_query"]:
                self.add_query(query_to_add)

        if "combine_query" in self.query_mapping:
            for query_info in self.query_mapping["combine_query"]:
                self.add_combined_query(query_info)

        if "queries_to_ask" in self.query_mapping:
            for key in self.query_dict.keys():
                if key not in self.query_mapping["queries_to_ask"]:
                    del(self.query_dict[key])

        return self.query_dict

    def add_query(self, query_info):
        if "operator" not in query_info:
                query_info["operator"] = " OR "
        # shorthand
        if (not "source_field" in query_info) and (
            not "query_field" in query_info) and (
                "source_query_field" in query_info):
                query_info["source_field"] = query_info["source_query_field"]
                query_info["query_fields"] = [{
                      "query_field": query_info["source_query_field"]}]
        query = self.create_query(self.document, **query_info)
        if query:
            self.query_dict[query_info["query_name"]] = query

    def add_combined_query(self, query_info):
        operator = " AND "
        name = query_info["name"]
        queries = []
        for query_name in query_info['query_names']:
            if query_name in self.query_dict:
                queries.append(self.query_dict[query_name])
        if "operator" in query_info:
            operator = query_info['operator']
        query = self.combine_query(queries, operator)
        self.query_dict[name] = query
