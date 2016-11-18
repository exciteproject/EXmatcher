# -*- coding: UTF-8 -*-
import bibtexparser
import glob, os
import string


def bibtex_parser(Path_source='./pdf_collection/bibtex_collection'):
    list_of_files = []
    list_of_refs = []
    os.chdir(Path_source)

    for file in glob.glob("*.bib"):
        list_of_files.append(file)

    for filename in list_of_files:
        with open(Path_source + '/' + filename) as bibtex_file:
            bibtex_str = bibtex_file.read()

        bib_database = bibtexparser.loads(bibtex_str)
        for item1 in bib_database.entries:
            list_of_refs.append(item1)
    return list_of_refs


def normauthors_alg(item):
    normalizedtitle = "".join(l for l in item if l not in string.punctuation)
    normalizedtitle = normalizedtitle.lower()
    normalizedtitle = normalizedtitle.replace(" ", "")

    normalizedtitle=normalizedtitle.replace(u'ü', '')
    normalizedtitle=normalizedtitle.replace(u'ä', '')
    normalizedtitle=normalizedtitle.replace(u'ö', '')
    normalizedtitle=normalizedtitle.replace(u'ß', 'ss')
    normalizedtitle =''.join([i if ord(i) < 128 else '' for i in normalizedtitle])
    return normalizedtitle


def Author_title(item):
    normalizedtitle = "".join(l for l in item['title'] if l not in string.punctuation)

    normalizedtitle = normalizedtitle.lower()

    normalizedtitle = normalizedtitle.replace(" ", "")
    normalizedtitle=normalizedtitle.replace(u'ü', 'u')
    normalizedtitle=normalizedtitle.replace(u'ä', 'a')
    normalizedtitle=normalizedtitle.replace(u'ö', 'o')
    normalizedtitle=normalizedtitle.replace(u'ß', 'ss')

    normalizedtitle =''.join([i if ord(i) < 128 else '' for i in normalizedtitle])

    autors = item['author'].split(',')
    norm_authors=[]
    for item in autors:
        norm_authors.append(normauthors_alg(item))

    return normalizedtitle, norm_authors

