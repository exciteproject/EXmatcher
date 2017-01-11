__author__ = "ottowg, Behnam Ghavimi"
import pandas as pd
import binascii
import random
from itertools import combinations as comb
import time
from multiprocessing import Pool

max_shingle_id = 2**32-1
next_prime = 4294967311

def generate_coeffs(num_hashes=100):
    """
    Generate the coefficients for the hash functions
    @param num_hashes:  The number of coeffiecient you want to generate
                        (k in literature)
    @return A List of tuples of two randomly generated coefficients
            with the size num_hashes
            Example: [(302934, 2389881),(234209, 938990)]
    """
    coeff_a = list()
    while len(coeff_a) < 2*(num_hashes+1):
        coeff_a.append(random.randint(0, max_shingle_id))
    tem_lent=int(len(coeff_a) / 2)
    coeff_a_1 = coeff_a[:tem_lent]
    coeff_a_2 = coeff_a[tem_lent:]
    coeffs = [c for c in zip(coeff_a_1, coeff_a_2)]
    return coeffs

start_time = time.time()
#Generate and save coefficients
COEFFS=generate_coeffs()
df3 = pd.DataFrame(COEFFS)
df3.to_csv('list_of_coeeff.csv', index=False)
print("--- %s minutes --- save_Coeff" % ((time.time() - start_time)/60))

def main():

    start_time = time.time()
    #load data from collected data from solr
    titles = pd.read_csv("norm_title_str-23-11-22.csv")

    print("--- %s minutes --- loading data" % ((time.time() - start_time)/60))
    start_time = time.time()

    #re-index data based on solr-id
    titles = titles.set_index(["norm_publishDate_str", "norm_title_str"],
                              drop=False).sort_index().set_index("id", drop=False)

    print("--- %s minutes --- sorting data" % ((time.time() - start_time)/60))
    start_time = time.time()

    #Predefined constants for calculation
    num_hashes = 100
    n = 3
    number_of_test_items = 10000000
    start = 0

    # Creating the hashes
    titles = titles[start:start + number_of_test_items]

    titles = add_hashes_multi(titles, cores = 30)

    print("--- %s minutes --- create hashes" % ((time.time() - start_time)/60))
    start_time = time.time()

    #save generated hashes for solr-items
    titles.to_csv("title_hashes_24-11-2016.csv", index=False)

    print("--- %s minutes --- save hashes" % ((time.time() - start_time)/60))
    start_time = time.time()


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


def str_to_nr(text):
    #convert string to a number value
    #in python2 ( crc32(bytes(text) )
    return binascii.crc32(bytes(text, "utf-8")) & 0xffffffff



def hash_int(shingles, coeff):
    #generate a min_hash value for a title by considering a min_hash function
    temresult=[]
    temresult.append(next_prime + 1)
    for s in shingles:
        cal=int(coeff[0] * s + coeff[1]) % next_prime
        temresult.append(cal)
    return min(temresult)
	
def compare_min_hash(one, two):
    """
    Comparing two hashes
    @return the approximated jaccard distance generated from two hashes
    """
	#jaccard comparision
    nr_same = sum([1 for nr in range(len(one)) if
                  one[nr] == two[nr]])
    return nr_same/len(one)



def add_hashes_multi(titles, cores=4):
    poll = Pool(processes=cores)
    result = poll.map(get_min_hash, titles.norm_title_str, 100)
    result = pd.DataFrame(result)
    result = result.set_index(titles.index)
    titles = titles.join(result)
    return titles


def get_min_hash(text, coeffs=COEFFS, n=3):
    shingles = get_shingles(text, n)
    return [hash_int(shingles, coeff) for
            coeff in coeffs]

if __name__ == '__main__':
    main()
