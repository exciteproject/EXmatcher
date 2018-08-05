import json
import psycopg2
from Features_extraction_support import *
import pickle
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC
import json

configfile = json.load(open('./configfile.json'))

def dtg(refsegdictitem, resultitem, reftextstring):
    authorfeatures = authores_features(refsegdictitem, resultitem)  # checked
    titlefeatures = title_features(refsegdictitem, resultitem)
    publishyearfeatures = publishyear_features(refsegdictitem, resultitem)
    pagefeatures = page_features(refsegdictitem, resultitem)
    numberfeatures = number_features(refsegdictitem, resultitem)
    journalfeatures = journal_features(refsegdictitem, resultitem)
    ls_vvd=sowiportfeatureinrefstring(refsegdictitem,resultitem, reftextstring)
    allfeatures = authorfeatures + titlefeatures + publishyearfeatures + pagefeatures + numberfeatures + journalfeatures+ls_vvd

    return allfeatures
    
    
def final_result():    
    
    clf = pickle.load(open("data_generated/finalized_modelsvm.sav", 'rb'))


    conn = psycopg2.connect("dbname="+configfile['dbname']+" user="+configfile['user']+" host="+configfile['host']+" password="+configfile['password'])
    cur = conn.cursor()
    cur.execute("select * from information_schema.tables where table_name=%s", ('join_segcit_resultmatchsowi',))
    if bool(cur.rowcount):
        cur.execute("DROP TABLE join_segcit_resultmatchsowi")
        conn.commit()

    cur.execute(
        "CREATE TABLE join_segcit_resultmatchsowi AS SELECT One.ref_id,One.resultfromsowi, One.sowi_id ,two.segcit_dict, two.ref_text FROM bna_records_table_sowimatch_test as One INNER JOIN BNA_source_table_match_test as two ON (two.ref_id = One.ref_id);")
    conn.commit()

    cur.execute("SELECT ref_id, sowi_id, resultfromsowi, segcit_dict,ref_text FROM join_segcit_resultmatchsowi")
    row = cur.fetchone()
    ls=[]
    while row is not None:
        #print(row[0], row[1], type(json.loads(row[2])), type(json.loads(row[3])))
        featuers=dtg(json.loads(row[3]),json.loads(row[2]),row[4])
        try:
            probib=clf.predict_proba([featuers])[0]
        except:
            print(featuers)
            break
    
        try:
          DATA_QUERY=(str(row[0]),str(row[1]),str(featuers),str(probib),str(clf.predict([featuers])[0]))
        except:
            print("error")
            exit()
        row = cur.fetchone()
        
        ls.append(DATA_QUERY)

    print("===========")

    for itemdata in ls:    
        query = """INSERT INTO Match_result_new_approach(ref_id,sowi_id,Featurs,prob,is_match) VALUES (%s, %s, %s, %s, %s)"""
        cur.execute(query, itemdata)  
        conn.commit()
    
    cur.close()
    conn.close()
    
final_result()