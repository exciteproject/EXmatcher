def list_of_keys_of_dicts_in_a_list(listtocheck):
    list_k_di=[]
    for item in listtocheck:
        try:
            list_k_di+=list(eval(item).keys())
            list_k_di=list(set(list_k_di))
        except:
            print(item)
    return list_k_di
    
    
def dict_generator(temp_val):
    dict_val_new={}
    for item_key in list(temp_val.keys()):
        #print(item_key)
        if item_key in ["author","title","issue","volume","page","year","source"]:
            if item_key=="author":
                authors=temp_val.get(item_key,[])
                temp_authors=[]
                for a_i in authors:
                    for a_i_i in a_i:
                        if "surname" in list(a_i_i.keys()):
                            temp_authors.append(a_i_i["surname"])
                dict_val_new["author"]=','.join(temp_authors)
                #print(temp_authors)
            elif item_key=="title":
                temp_title=[]
                titles=temp_val.get(item_key,[])
                for t_i in titles:
                    temp_title.append(t_i["value"])
                dict_val_new["title"]=" ".join(temp_title)
                #print(" ".join(temp_title))    
            elif item_key=="issue":
                temp_issue=[]
                issues=temp_val.get(item_key,[])
                for is_i in issues:
                    temp_issue.append(is_i["value"])
                dict_val_new["number"]=temp_issue
                #print(temp_issue)
            elif item_key=="volume":
                temp_volume=[]
                volumes=temp_val.get(item_key,[])
                for v_i in volumes:
                    temp_volume.append(v_i["value"])
                dict_val_new["volume"]=temp_volume
                #print(temp_volume)
            elif item_key=="page":
                temp_page=[]
                pages=temp_val.get(item_key,[])
                for p_i in pages:
                    temp_page.append(p_i["value"])
                dict_val_new["pages"]=",".join(temp_page)
                #print(temp_page)
            elif item_key=="year":
                temp_year=[]
                years=temp_val.get(item_key,[])
                for y_i in years:
                    temp_year.append(y_i["value"])
                dict_val_new["year"]=temp_year
                #print(temp_year)
            elif item_key=="source":
                temp_source=[]
                sources=temp_val.get(item_key,[])
                for s_i in sources:
                    temp_source.append(s_i["value"])
                dict_val_new["journal"]=" ".join(temp_source)
                #print(" ".join(temp_source))
            dict_val_new["ID"]="id_autogenrated"    
            #print("---------")    
            #print(temp_val.get(item_key,""))
    return dict_val_new
    
    
