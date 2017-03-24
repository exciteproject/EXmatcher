# -*- coding: UTF-8 -*-

__author__ = "Behnam Ghavimi"

import bibtexparser
import glob, os
import string
import re


def bibtex_parser(Path_source='./'):
    list_of_refs = []

    list_of_files=listoffile_retriver(Path_source)
    for filename in list_of_files:
        dnm=Path_source + '/' + filename
        with open(dnm) as bibtex_file:
            bibtex_str = bibtex_file.read()

        bib_database = bibtexparser.loads(bibtex_str)
        for item1 in bib_database.entries:
            list_of_refs.append([item1,dnm])
    return list_of_refs

def listoffile_retriver(Path_source):
    list_of_files = []
    os.chdir(Path_source)
    for file in glob.glob("*.bib"):
        list_of_files.append(file)
    return list_of_files

def listrefinafile(filename,Path_source):
    list_of_refs = []
    dnm = Path_source + '/' + filename
    with open(dnm) as bibtex_file:
        bibtex_str = bibtex_file.read()

    bib_database = bibtexparser.loads(bibtex_str)
    for item1 in bib_database.entries:
        list_of_refs.append([item1, dnm])
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


def filteryear(itemyear):
    year = ''
    try:
        if 1000<float(itemyear) and 3000>float(itemyear):
            year=itemyear
    except:
        pass
    return year

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
    for item1 in autors:
        norm_authors.append(normauthors_alg(item1))

    Year_item=item['year']
    listofyear=[]
    for itemyis in Year_item.split(','):
        for itemyis_s in itemyis.split(' '):
            listofyear.append(filteryear(itemyis_s))

    return normalizedtitle, norm_authors, item['ID'], listofyear

