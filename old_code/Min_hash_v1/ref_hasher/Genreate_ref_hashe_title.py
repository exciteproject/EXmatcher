# -*- coding: UTF-8 -*-
__author__ = "Behnam Ghavimi"
import pandas as pd
from time import time
import bibtexparser
import string
import glob, os

#source dir should be defined._ Co-efficient values.
data1 = pd.read_csv("source_dir")

#source dir should be defined. _ source folder of bibtex files
def bibtex_parser(Path_source=r'source_dir2'):
    list_of_files = []
    list_of_refs = []
    os.chdir(Path_source)

    for file in glob.glob("*.bib"):
        list_of_files.append(file)

    for filename in list_of_files:
        dnm=Path_source + '/' + filename
        with open(dnm) as bibtex_file:
            bibtex_str = bibtex_file.read()

        bib_database = bibtexparser.loads(bibtex_str)
        for item1 in bib_database.entries:
            list_of_refs.append([item1,dnm])
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

def normalize_input():
        t0 = time()
        list_of_refs = bibtex_parser()
        t1 = time()
        print ("loading all refrences in a file")
        print ((t0 - t1) / 60)

        t0 = time()
        Normlized_title_author_list = []
        for item in list_of_refs:
            if len(item[0]) > 1:
                try:
                    temprec = {}
                    ntitles, norm_authors = Author_title(item[0])
                    temprec["norm_title_str"] = ntitles
                    temprec["author_name"] = norm_authors
                    temprec["filename"] = item[1]
                    Normlized_title_author_list.append(temprec)
                except:
                    pass
        return Normlized_title_author_list


def ref_hash_genrater(data1,ref_intial):
    COEFFS = data1.as_matrix()
    COEFFS = COEFFS.tolist()
    lsref_hash = []
    for item_ref in ref_intial:
        minhash_list_ref_title = get_min_hash(item_ref['norm_title_str'], COEFFS, 3)
        item_ref['hash_values'] = minhash_list_ref_title
        lsref_hash.append(item_ref)
    return lsref_hash


import binascii
import random
next_prime = 4294967311
def str_to_nr(text):
    return binascii.crc32(bytes(text)) & 0xffffffff

def get_min_hash(text, coeffs, n=3):
    shingles = get_shingles(text, n)
    lsr=[]
    for coeff in coeffs:
        lsr.append(hash_int(shingles, coeff))
    return lsr

def get_shingles(text, n):
    """
    Get integer representation of each shingle of a
    text string
    @param text:    The text from which you want to get the represenation
    @param n:       The n for ngrams zou want to use
    """
    text = "" if type(text) is float else text
    return list({str_to_nr(text[max(0, p):p+n]) for
                 p in range(1-n, len(text))})

def hash_int(shingles, coeff):
    temresult=[]
    temresult.append(next_prime + 1)
    for s in shingles:
        cal=(coeff[0] * s + coeff[1]) % next_prime
        temresult.append(int(cal))
    return min(temresult)

Ref_norm=normalize_input()
list_with_hashes=ref_hash_genrater(data1,Ref_norm)
df3 = pd.DataFrame(list_with_hashes)
df3.to_csv('target_dir\list_ref_hashes_with_correction.csv', index=False)
print(list_with_hashes[1])