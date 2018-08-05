import re
import string


def filteryear_new(itemyear):
    year = ''
    try:
        itemyear = re.findall('\d+', itemyear)
        itemyear = itemyear[0]
        if 1000 < int(itemyear) and 3000 > int(itemyear):
            year = itemyear
    except:
        pass
    return year


def normalizeinput_year_new(Year_item):
    Year_item = str(Year_item)
    listofyear = []
    listof_replace = re.findall('[a-zA-Z]', Year_item)
    listof_replace = list(set(listof_replace))
    for item in listof_replace:
        Year_item = Year_item.replace(item, " ")
    for itemyis in Year_item.split(','):
        for itemyis_s in itemyis.split(' '):
            item_iter_year = filteryear_new(itemyis_s)
            if item_iter_year != '':
                listofyear.append(item_iter_year.replace(" ", ""))
    return listofyear


def norm_number(stringnumber):
    finalreturn = []
    listofnumber = stringnumber.split(",")
    for item in listofnumber:
        finalreturn = finalreturn + re.findall('\d+', item)
    return ",".join(list(set(finalreturn)))


def normalizeinput_title_new(titlestr):
    try:
        titlestr = str(float(titlestr))
    except:
        pass
    final_result = []
    for item in titlestr:
        normalizedtitle = "".join(l for l in item if l not in string.punctuation)
        normalizedtitle = normalizedtitle.lower()
        normalizedtitle = normalizedtitle.replace(" ", "")
        normalizedtitle = normalizedtitle.replace(u'ü', 'u')
        normalizedtitle = normalizedtitle.replace(u'ä', 'a')
        normalizedtitle = normalizedtitle.replace(u'ö', 'o')
        normalizedtitle = normalizedtitle.replace(u'ß', 'ss')
        normalizedtitle = ''.join([i if ord(i) < 128 else '' for i in normalizedtitle])
        if (normalizedtitle.strip() != ""):
            final_result.append(normalizedtitle.strip())
    return final_result


def filterauthor(item):
    normalizedtitle = "".join(l for l in item if l not in string.punctuation)
    normalizedtitle = normalizedtitle.lower()
    normalizedtitle = normalizedtitle.strip()
    normalizedtitle = normalizedtitle.replace(" ", "")

    normalizedtitle = normalizedtitle.replace(u'ü', '')
    normalizedtitle = normalizedtitle.replace(u'ä', '')
    normalizedtitle = normalizedtitle.replace(u'ö', '')
    normalizedtitle = normalizedtitle.replace(u'ß', 'ss')
    normalizedtitle = ''.join([i if ord(i) < 128 else '' for i in normalizedtitle])
    return normalizedtitle


def normalizeinput_author(autors):
    try:
        autors = str(float(autors))
    except:
        pass

    autors = autors.split(',')
    norm_authors = []
    for item1 in autors:
        norm_item_iter = filterauthor(item1)
        if len(norm_item_iter) > 1:
            norm_authors.append(norm_item_iter)
    return norm_authors

def normalizeinput_author1(autors):
    try:
        autors = str(float(autors))
    except:
        pass

    autors = autors.split(',')
    norm_authors = []
    for item1 in autors:
        norm_item_iter = filterauthor(item1)
        if len(norm_item_iter) > 0:
            norm_authors.append(norm_item_iter)
    return norm_authors
    
def page_normaliser(listofpage):
    final_list = []
    for item in listofpage:
        if "-" in item:
            item = item.replace(" -", "-").replace("- ", "-").replace("--", "-")
            templist = re.findall("\d+-\d+", item)
        else:
            templist = re.findall("\d+", item)
        final_list += templist
    return final_list
