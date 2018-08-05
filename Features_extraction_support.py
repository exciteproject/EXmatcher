import difflib
from difflib import ndiff
from difflib import SequenceMatcher

import cologne_phonetics
from aux_function import *
from matcher_query.normalizer_function import *


def levenshtein(a, b):
    "Calculates the Levenshtein distance between a and b."
    n, m = len(a), len(b)
    if n > m:
        # Make sure n <= m, to use O(min(n,m)) space
        a, b = b, a
        n, m = m, n

    current = range(n + 1)
    for i in range(1, m + 1):
        previous, current = current, [i] + [0] * n
        for j in range(1, n + 1):
            add, delete = previous[j] + 1, current[j - 1] + 1
            change = previous[j - 1]
            if a[j - 1] != b[i - 1]:
                change = change + 1
            current[j] = min(add, delete, change)

    return 1 - (current[n] / max(len(a), len(b)))


def levenshtein1(a, b):
    "Calculates the Levenshtein distance between a and b."
    n, m = len(a), len(b)
    if n > m:
        # Make sure n <= m, to use O(min(n,m)) space
        a, b = b, a
        n, m = m, n

    current = range(n + 1)
    for i in range(1, m + 1):
        previous, current = current, [i] + [0] * n
        for j in range(1, n + 1):
            add, delete = previous[j] + 1, current[j - 1] + 1
            change = previous[j - 1]
            if a[j - 1] != b[i - 1]:
                change = change + 1
            current[j] = min(add, delete, change)

    return 1 - (current[n] / min(len(a), len(b)))


def jaccard_similarity(list1, list2):
    intersection = len(list(set(list1).intersection(list2)))
    union = (len(list1) + len(list2)) - intersection
    return float(intersection / union)


def compare_strings(string_one, string_two):
    """
    """
    s_short, s_long = sorted([string_one, string_two], key=lambda x: len(x))
    s_short = s_short.split()
    s_long = s_long.split()
    deleted_from_short = 0
    added_to_long = 0
    small_dif_word = 0

    for i in ndiff(s_short, s_long):
        if i[0] == "+":
            added_to_long += 1
        if i[0] == "-":
            deleted_from_short += 1
        if i[0] == "?":
            small_dif_word += 1
            # print(i)
    from_short_in_long_in_short = 1 - ((
                                           deleted_from_short - 0.5 * small_dif_word) / len(s_short))
    from_short_in_long_in_long = 1 - ((
                                          added_to_long - 0.5 * small_dif_word) / len(s_long))
    return from_short_in_long_in_short, from_short_in_long_in_long


def generate_ngrams(words_list, n):
    ngrams_list = []

    for num in range(0, len(words_list)):
        ngram = ''.join(words_list[num:num + n])
        if len(ngram) > n - 1:
            ngrams_list.append(ngram)

    return ngrams_list


def aux_author_features_gen(segcite, sowiport):
    authorlist_segcite = segcite.get("author", [])
    dict_aut_edit_surename = {}
    lastname_score_dict_segcit = {}
    flag = 0
    firstauthor = {}
    ls_firstname = []
    ls_lastname = []
    count_fn = 0
    for aut_segcit in authorlist_segcite:
        for eachpart_name in aut_segcit:
            temp = eachpart_name.get("surname", "")
            if temp != "":
                sn = eachpart_name.get("score", "")
                lastname_score_dict_segcit[temp] = sn
                ls_lastname.append(temp)
                if flag == 0:
                    firstauthor = {temp: sn}
                    flag = 1
                count_fn += 1
            # ***************************************
            temp1 = eachpart_name.get("given-names", "")
            if temp1 != "":
                temp2 = eachpart_name
                temp2["index"] = count_fn
                ls_firstname.append(temp2)

    index_i_fn = -1
    aux_list = []
    aux_dict_fn = {}
    count = 0

    for index, item in enumerate(ls_firstname):

        if index_i_fn == item["index"]:

            if len(ls_firstname) - 1 != index:
                aux_list.append(item["given-names"])
            else:
                aux_list.append(item["given-names"])
                aux_dict_fn[count] = aux_list


        elif index_i_fn == -1:

            if len(ls_firstname) - 1 != index:

                index_i_fn = item["index"]
                aux_list.append(item["given-names"])

            else:

                aux_list.append(item["given-names"])
                aux_dict_fn[count] = aux_list

        else:

            if len(ls_firstname) - 1 != index:

                aux_dict_fn[count] = aux_list
                index_i_fn = item["index"]
                aux_list = []
                count += 1
            else:

                aux_dict_fn[count] = aux_list
                index_i_fn = item["index"]
                count += 1
                aux_list = []
                aux_list.append(item["given-names"])
                aux_dict_fn[count] = aux_list

    normalized_sowiport = sowiport.get("person_author_normalized_str_mv", [])

    # print("--------")
    sowiportnameslist = sowiport.get("facet_person_str_mv", [])
    listoffamilysowi = []
    listoffirestnamesowi = []
    new_facet_list_author = []
    for index, item in enumerate(sowiportnameslist):
        templist = item.split(",")
        new_facet_list_author.append(templist)
        if index < len(normalized_sowiport):
            templist = [x.lower().strip() for x in templist]
            fn_nn = difflib.get_close_matches(normalized_sowiport[index], templist)
            if len(fn_nn)>0:
                listoffamilysowi.append(fn_nn[0])
                templist.remove(fn_nn[0])
            listoffirestnamesowi.append(templist)

    return {"ln_dict_score": lastname_score_dict_segcit, "first_aut_ln": firstauthor, "gn_aux_dict": aux_dict_fn,
            "ln_ls_order": ls_lastname, "sowi_facet_names_ls": sowiportnameslist,
            "sowi_ln_norm_ls": normalized_sowiport, "sowi_ls_ls": listoffamilysowi, "sowi_gn_ls": listoffirestnamesowi,
            "sowi_aux_facet": new_facet_list_author}


def authores_features(segcite, sowiport):
    authorlist_segcite = segcite.get("author", [])
    print(authorlist_segcite)
    editorlist_segcite = segcite.get("editor", [])
    print(editorlist_segcite)
    print("*********")
    print(sowiport.get("id", "id"))
    print(sowiport.get("person_author_normalized_str_mv", "no_record"))
    print(sowiport.get("facet_person_str_mv", "no_record"))


def publishyear_features(segcite, sowiport):
    if len(segcite.get("year", [])) > 0 and len(sowiport.get("norm_publishDate_str", [])) > 0:
        Foo = 1
    else:
        Foo = 0

    dict_year = {}
    for item in segcite.get("year", []):
        if len(normalizeinput_year_new(item["value"])):
            dict_year[normalizeinput_year_new(item["value"])[0]] = item["score"]
    sowi_value = str(sowiport.get("norm_publishDate_str", ""))
    if sowi_value in list(dict_year.keys()):
        Match_ext = 1
        Prob_year = float(dict_year[sowi_value])
    else:
        Match_ext = 0
        Prob_year = 0

    return [Match_ext, Prob_year, Foo]


def page_features(segcite, sowiport):
    if len(segcite.get("page", [])) > 0 and sowiport.get("norm_pagerange_str", "") != "":
        Foo = 1
    else:
        Foo = 0
    seg_page_dict = {}
    for item in segcite.get("page", []):
        seg_page_dict[item["value"]] = item["score"]

    sowi_pages = re.findall("\d+", str(sowiport.get("norm_pagerange_str", "")))
    inter_list = list(set(seg_page_dict).intersection(set(sowi_pages)))
    sum_pr = 0
    for item in inter_list:
        sum_pr += float(seg_page_dict[item])

    Total_nr = len(set(list(seg_page_dict.keys()) + sowi_pages))

    if Total_nr != 0:
        jacar_pr = sum_pr / Total_nr
    else:
        jacar_pr = 0

    return [Foo, jacar_pr]


def number_features(segcite, sowiport):
    seg_data = segcite.get("volume", []) + segcite.get("issue", [])
    dict_seg_data = {}
    for item in seg_data:
        dict_seg_data[item["value"]] = item["score"]

    sowi_data = re.findall("\d+", str(sowiport.get("norm_issue_str", []))) + re.findall("\d+", str(
        sowiport.get("norm_volume_str", [])))

    if len(list(dict_seg_data.keys())) > 0 and len(sowi_data) > 0:
        Foo = 1
    else:
        Foo = 0

    intersect_data = []
    wholelen = len(set(list(dict_seg_data.keys()) + sowi_data))

    interitem = list(set(dict_seg_data.keys()).intersection(set(sowi_data)))

    ls_data_inter_pr = []
    for item in interitem:
        ls_data_inter_pr.append(float(dict_seg_data[item]))

    if wholelen > 0 and len(ls_data_inter_pr) > 0:
        jacard = sum(ls_data_inter_pr) / wholelen
    else:
        jacard = 0

    return [jacard, Foo]


def title_features(segcite, sowiport):
    ls_seg_title = []
    for item_title in segcite.get('title', []):
        ls_seg_title.append(item_title["value"])
    ls_sowi_title_full_str = sowiport.get("title_full", "")
    ls_sowi_title_full = ls_sowi_title_full_str.split(" ")
    sub_sowi_title_str = sowiport.get("title_sub", "")
    sub_sowi_title = sub_sowi_title_str.split(" ")
    if sub_sowi_title != "":
        delta = ls_sowi_title_full_str.replace(sub_sowi_title_str, "").split(" ")

    if len(ls_seg_title) > 0:
        if len(ls_sowi_title_full) > 0 or len(sub_sowi_title) > 0:
            seg_title_ls = []
            for x in ls_seg_title:
                tnt = normalizeinput_title_new([x])
                if len(tnt) > 0:
                    seg_title_ls.append(tnt[0])

            sowi_title_ls = []
            for x in ls_sowi_title_full:
                tnt = normalizeinput_title_new([x])
                if len(tnt) > 0:
                    sowi_title_ls.append(tnt[0])

            sub_sowi_title_ls = []
            for x in sub_sowi_title:
                tnt = normalizeinput_title_new([x])
                if len(tnt) > 0:
                    sub_sowi_title_ls.append(tnt[0])

            delta_ls = []
            for x in delta:
                tnt = normalizeinput_title_new([x])
                if len(tnt) > 0:
                    delta_ls.append(tnt[0])

            leven_pr_exact1 = levenshtein(seg_title_ls, sowi_title_ls)
            # print(leven_pr_exact1)
            leven_pr_exact2 = levenshtein(seg_title_ls, sub_sowi_title_ls)
            # print(leven_pr_exact2)
            leven_pr_exact3 = levenshtein(seg_title_ls, delta_ls)
            # print(leven_pr_exact3)
            leven_pr_exact = max(leven_pr_exact1, leven_pr_exact2, leven_pr_exact3)

            seg_dict_title_con = {}
            for item in segcite.get('title', []):
                seg_dict_title_con[item['value']] = float(item['score'])

            intersect = list(set(seg_title_ls).intersection(set(sowi_title_ls)))
            # ==============================
            keys_ls_dict = list(seg_dict_title_con.keys())
            ls_pr_itt_temp = []
            for item_key in keys_ls_dict:
                if item_key.lower() in intersect:
                    ls_pr_itt_temp.append(seg_dict_title_con[item_key])
            Jacard_pr = sum(ls_pr_itt_temp) / len(set(seg_title_ls + sowi_title_ls))
            foo = 1

            trigramsegcit = generate_ngrams("".join(seg_title_ls), 3)
            trigramsowititle = generate_ngrams("".join(sowi_title_ls), 3)
            trigramsdelta = generate_ngrams("".join(delta_ls), 3)
            trigramsubsowi = generate_ngrams("".join(sub_sowi_title_ls), 3)
            trlev1_pr = levenshtein(trigramsegcit, trigramsowititle)
            trlev2_pr = levenshtein(trigramsegcit, trigramsdelta)
            trlev3_pr = levenshtein(trigramsegcit, trigramsubsowi)

            trilev_pr = max(trlev1_pr, trlev2_pr, trlev3_pr)

    else:
        Jacard_pr = 0
        leven_pr_exact = 0
        foo = 0
        trilev_pr = 0
    return [Jacard_pr, leven_pr_exact, trilev_pr, foo]


def journal_features(segcite, sowiport):
    abbrivationjournal = sowiport.get("zsabk_str", "zsabk_str_notmatch")
    ls_seg_title = []
    for item_title in segcite.get('title', []):
        ls_seg_title.append(item_title["value"])
    if abbrivationjournal in " ".join(ls_seg_title):
        Faoo = 1
    else:
        Faoo = 0

    ls_sowi_title_full_str = sowiport.get("journal_title_txt_mv", [])
    if len(ls_sowi_title_full_str) > 0:
        ls_sowi_title_full_str = ls_sowi_title_full_str[0]
    else:
        ls_sowi_title_full_str = ""
    ls_sowi_title_full = ls_sowi_title_full_str.split(" ")

    sub_sowi_title_str = sub_sowi_title_str = sowiport.get("journal_short_txt_mv", [])
    if len(sub_sowi_title_str) > 0:
        sub_sowi_title_str = sub_sowi_title_str[0]
    else:
        sub_sowi_title_str = ""
    sub_sowi_title = sub_sowi_title_str.split(" ")

    if len(ls_seg_title) > 0:
        if len(ls_sowi_title_full) > 0 or len(sub_sowi_title) > 0:
            Foo = 1

            norm_ls_seg_title = []
            for item in ls_seg_title:
                tnt = normalizeinput_title_new([item])
                if len(tnt) > 0:
                    norm_ls_seg_title.append(tnt[0])

            norm_ls_sowi_title_full = []
            for item in ls_sowi_title_full:
                tnt = normalizeinput_title_new([item])
                if len(tnt) > 0:
                    norm_ls_sowi_title_full.append(tnt[0])

            norm_sub_sowi_title = []
            for item in sub_sowi_title:
                tnt = normalizeinput_title_new([item])
                if len(tnt) > 0:
                    norm_sub_sowi_title.append(tnt[0])
            if len(norm_ls_seg_title) > 0 and len(norm_ls_sowi_title_full) > 0:
                leven_pr_exact1 = levenshtein(norm_ls_seg_title, norm_ls_sowi_title_full)
                intsect_t = set(norm_ls_seg_title).intersection(set(norm_ls_sowi_title_full))
                jacardmin = float(len(intsect_t)) / min(len(norm_ls_seg_title), len(norm_ls_sowi_title_full))
                jacardmax = float(len(intsect_t)) / max(len(norm_ls_seg_title), len(norm_ls_sowi_title_full))
            else:
                leven_pr_exact1 = 0
                jacardmin = 0
                jacardmax = 0

            if len(norm_ls_seg_title) > 0 and len(norm_sub_sowi_title) > 0:
                leven_pr_exact2 = levenshtein(norm_ls_seg_title, norm_sub_sowi_title)
                intsect_t = set(norm_ls_seg_title).intersection(set(norm_sub_sowi_title))
                jacardmin1 = float(len(intsect_t)) / min(len(norm_ls_seg_title), len(norm_sub_sowi_title))
                jacardmax1 = float(len(intsect_t)) / max(len(norm_ls_seg_title), len(norm_sub_sowi_title))
            else:
                leven_pr_exact2 = 0
                jacardmin1 = 0
                jacardmax1 = 0

            leven_pr = max(leven_pr_exact1, leven_pr_exact2)
            jacardmin = max(jacardmin, jacardmin1)
            jacardmax = max(jacardmax, jacardmax1)

        else:
            Foo = 0
            leven_pr = 0
            jacardmin = 0
            jacardmax = 0


    else:
        Foo = 0
        leven_pr = 0
        jacardmin = 0
        jacardmax = 0

    return [Faoo, Foo, leven_pr, jacardmin, jacardmax]


def authores_features(segcite, sowiport):
    prep_data_ss = aux_author_features_gen(segcite, sowiport)

    # First author features
    # ============================
    if len(list(prep_data_ss["first_aut_ln"])) > 0 and len(prep_data_ss['sowi_ln_norm_ls']):
        First_author_OO = 1
        if normalizeinput_author1(prep_data_ss["ln_ls_order"][0])[0] == prep_data_ss["sowi_ln_norm_ls"][0]:
            First_author_Exact = 1
        else:
            First_author_Exact = 0

        if len(prep_data_ss["ln_ls_order"]) > 0 and len(prep_data_ss["sowi_ls_ls"]) > 0:
            if cologne_phonetics.encode(prep_data_ss["ln_ls_order"][0])[0][1] == \
                    cologne_phonetics.encode(prep_data_ss["sowi_ls_ls"][0])[0][1]:
                First_author_Phono = 1
            else:
                First_author_Phono = 0
        else:
            First_author_Phono = 0
        Pro_seg_fa = float(list(prep_data_ss['first_aut_ln'].values())[0])
    else:
        First_author_OO = 0
        First_author_Exact = 0
        First_author_Phono = 0
        lsfatemp = list(prep_data_ss['first_aut_ln'].values())
        if len(lsfatemp) > 0:
            Pro_seg_fa = float(lsfatemp[0])
        else:
            Pro_seg_fa = 0

    # levenstien Exact/Phono
    # ==============================
    lower_ls_sowi_ls = [x.lower().strip() for x in prep_data_ss['sowi_ls_ls']]
    lower_ls_segcite_ls = [x.lower().strip() for x in prep_data_ss['ln_ls_order']]

    phono_ls_sowi_ls = [cologne_phonetics.encode(x.lower().strip())[0][1] for x in prep_data_ss['sowi_ls_ls']]
    phono_ls_seg_ls = [cologne_phonetics.encode(x.lower().strip())[0][1] for x in prep_data_ss['ln_ls_order']]
    if len(lower_ls_sowi_ls) > 0 and len(lower_ls_segcite_ls) > 0:
        if len(prep_data_ss['sowi_ls_ls']) > 0 and len(prep_data_ss['sowi_gn_ls']) > 0 and len(
                prep_data_ss['ln_ls_order']) and len(prep_data_ss['gn_aux_dict']):
            try:
                ln_x = []
                for inx, dls in enumerate(prep_data_ss['ln_ls_order']):
                    ln_x.append(
                        normalizeinput_author1(dls)[0] + prep_data_ss['gn_aux_dict'].get(inx, [""])[0][0].lower())
                ln_y = []
                for ixy, it_ls in enumerate(prep_data_ss['sowi_ls_ls']):
                    ln_y.append(normalizeinput_author1(it_ls)[0] + prep_data_ss['sowi_gn_ls'][ixy][0][0])
                leven_pr_exact = levenshtein(ln_x, ln_y)
            except:
                leven_pr_exact = levenshtein(lower_ls_sowi_ls, lower_ls_segcite_ls)
        else:
            leven_pr_exact = levenshtein(lower_ls_sowi_ls, lower_ls_segcite_ls)
        leven_pr_phono = levenshtein(phono_ls_sowi_ls, phono_ls_seg_ls)

        Jacard_pr_exact = jaccard_similarity(lower_ls_sowi_ls, lower_ls_segcite_ls)
        jacard_pr_phono = jaccard_similarity(phono_ls_sowi_ls, phono_ls_seg_ls)
        author_oo = 1
    else:
        leven_pr_exact = 0
        leven_pr_phono = 0
        Jacard_pr_exact = 0
        jacard_pr_phono = 0
        author_oo = 0

    intersect = list(set(lower_ls_sowi_ls).intersection(set(lower_ls_segcite_ls)))
    # ==============================
    keys_ls_dict = list(prep_data_ss["ln_dict_score"].keys())
    ls_pr_ln_temp = []
    for item_key in keys_ls_dict:
        if item_key.lower() in intersect:
            ls_pr_ln_temp.append(float(prep_data_ss["ln_dict_score"][item_key]))
    if len(ls_pr_ln_temp) > 0:
        jacard_pr_exact_scr = sum(ls_pr_ln_temp) / len(set(lower_ls_sowi_ls + lower_ls_sowi_ls))
    else:
        jacard_pr_exact_scr = 0

    return [First_author_OO, author_oo, First_author_Exact, First_author_Phono, Pro_seg_fa, leven_pr_exact,
            leven_pr_phono, Jacard_pr_exact, jacard_pr_phono, jacard_pr_exact_scr]

            
def sowiportfeatureinrefstring(refsegdictitem, resultitem, reftextstring):
    keys_insowiport=list(resultitem.keys())
    yearlistinreftext=normalizeinput_year_new(reftextstring)
    listofnumbersinreftext=re.findall(r'\d+',reftextstring)
    normalizerefstring=normalizeinput_title_new([reftextstring])
           
    if 'norm_volume_str' in keys_insowiport:
        nvs=re.findall(r'\d+',str(resultitem.get('norm_volume_str',"")))
        falgnvs=set(nvs).intersection(set(listofnumbersinreftext))
        if nvs=="":
            onoff_vol=0
        else:
            onoff_vol=1
            
        if len(list(falgnvs))>0:
            falgnvols=1
        else:
            falgnvols=0  
    else:
        onoff_vol=0
        falgnvols=0 
    #===================================================================         
    if 'title_full' in keys_insowiport:
        string1=str(normalizeinput_title_new(resultitem.get("title_full","")))
        if len(string1)>0:
            string2=str(normalizerefstring)
            match = SequenceMatcher(None, string1, string2).find_longest_match(0, len(string1), 0, len(string2))
            ratio_full_title_facet=len(string1[match.a: match.a + match.size])/len(string1)
            onoff_tf=1
        else:
            ratio_full_title_facet=0
            onoff_tf=0
    else:
        ratio_full_title_facet=0
        onoff_tf=0
    #===================================================================         
    if 'norm_issue_str' in keys_insowiport:
        nvs=re.findall(r'\d+',str(resultitem.get('norm_issue_str',"")))
        falgnvs=set(nvs).intersection(set(listofnumbersinreftext))
        if nvs=="":
            onoff_issue=0
        else:
            onoff_issue=1
            
        if len(list(falgnvs))>0:
            flagissue=1
        else:
            flagissue=0
    else:
        onoff_issue=0
        flagissue=0
    #===================================================================     
    if 'norm_title_str' in keys_insowiport:
        string1=str(resultitem.get("norm_title_str",""))
        if len(string1)>0:
            string2=str(normalizerefstring)
            match = SequenceMatcher(None, string1, string2).find_longest_match(0, len(string1), 0, len(string2))
            ratio_title_norm=len(string1[match.a: match.a + match.size])/len(string1)
            onoff_tfnorm=1
        else:
            ratio_title_norm=0
            onoff_tfnorm=0
    else:
        ratio_title_norm=0
        onoff_tfnorm=0
    #============================================================     
    if 'norm_pagerange_str' in keys_insowiport:
        nvs=re.findall(r'\d+',str(resultitem.get('norm_pagerange_str',"")))
        falgpage=set(nvs).intersection(set(listofnumbersinreftext))
        if nvs=="":
            onoff_page=0
        else:
            onoff_page=1
            
        if len(list(falgpage))>0:
            falgpage=1
        else:
            falgpage=0
    else:
        onoff_page=0
        falgpage=0
    #============================================================    
    if 'zsabk_str' in keys_insowiport:
        string1=str(normalizeinput_title_new(resultitem.get("zsabk_str","")))
        if len(string1)>0:
            string2=str(normalizerefstring)
            match = SequenceMatcher(None, string1, string2).find_longest_match(0, len(string1), 0, len(string2))
            ratio_zsbk=len(string1[match.a: match.a + match.size])/len(string1)
            onoff_zsbk=1
        else:
            ratio_zsbk=0
            onoff_zsbk=0
    else:
            ratio_zsbk=0
            onoff_zsbk=0
    #===========================================================    
    if 'journal_short_txt_mv' in keys_insowiport:
        string1=str(normalizeinput_title_new(resultitem.get("journal_short_txt_mv","")))
        if len(string1)>0:
            string2=str(normalizerefstring)
            match = SequenceMatcher(None, string1, string2).find_longest_match(0, len(string1), 0, len(string2))
            ratio_norm_jts=len(string1[match.a: match.a + match.size])/len(string1)
            onoff_jts=1
        else:
            ratio_norm_jts=0
            onoff_jts=0
    else:
        ratio_norm_jts=0
        onoff_jts=0 
    #==========================================================    
    if 'title_sub' in keys_insowiport:
        string1=str(normalizeinput_title_new(resultitem.get("title_sub","")))
        if len(string1)>0:
            string2=str(normalizerefstring)
            match = SequenceMatcher(None, string1, string2).find_longest_match(0, len(string1), 0, len(string2))
            ratio_norm_titlesub=len(string1[match.a: match.a + match.size])/len(string1)
            onoff_ts=1
        else:
            ratio_norm_titlesub=0
            onoff_ts=0
    else:
        ratio_norm_titlesub=0
        onoff_ts=0 
    #==========================================================       
    if 'journal_title_txt_mv' in keys_insowiport:
        string1=str(normalizeinput_title_new(resultitem.get("journal_title_txt_mv","")))
        if len(string1)>0:
            string2=str(normalizerefstring)
            match = SequenceMatcher(None, string1, string2).find_longest_match(0, len(string1), 0, len(string2))
            ratio_norm_jt=len(string1[match.a: match.a + match.size])/len(string1)
            onoff_jt=1
        else:
            ratio_norm_jt=0
            onoff_jt=0
    else:
        ratio_norm_jt=0
        onoff_jt=0
    #==========================================================       
    if 'norm_publishDate_str' in keys_insowiport:
        nvs=re.findall(r'\d+',str(resultitem.get('norm_publishDate_str',"")))
        falgyear=set(nvs).intersection(set(yearlistinreftext))
        if nvs=="":
            onoff_datayear=0
        else:
            onoff_datayear=1
            
        if len(list(falgyear))>0:
            falgyear=1
        else:
            falgyear=0 
    else:
        onoff_datayear=0
        falgyear=0 
    #==========================================================       
    if 'norm_title_full_str' in keys_insowiport:
        string1=str(resultitem.get("norm_title_full_str",""))
        if len(string1)>0:
            string2=str(normalizerefstring)
            match = SequenceMatcher(None, string1, string2).find_longest_match(0, len(string1), 0, len(string2))
            ratio_norm_title_full=len(string1[match.a: match.a + match.size])/len(string1)
            onoff_nff=1
        else:
            ratio_norm_title_full=0
            onoff_nff=0
    else:
        ratio_norm_title_full=0
        onoff_nff=0 
    #==========================================================       
        
    return [onoff_nff,ratio_norm_title_full,falgyear,onoff_datayear,onoff_jt,ratio_norm_jt,onoff_ts,ratio_norm_titlesub,onoff_jts,ratio_norm_jts,onoff_zsbk,ratio_zsbk,falgpage,onoff_page,onoff_tfnorm,ratio_title_norm,flagissue,onoff_issue,
            onoff_tf,ratio_full_title_facet,falgnvols,onoff_vol]