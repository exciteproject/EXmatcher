# -*- coding: utf-8 -*-
import psycopg2
import binder as bd
import sys
import time
import datetime

reload(sys)
sys.setdefaultencoding('utf8')

# Comment: one select and then one insert
def main():
    try:
        LOG = "./logfile.log"
        logf = open(LOG, "a")
        t0 = time.time()
        conn = None
        conn = psycopg2.connect("dbname='' user='' host='' password=''")
        cur = conn.cursor()
        cur.execute("""SELECT * FROM ES_newmodel_Bibtexes """)
        rows = cur.fetchall()
        input_dict = {}
        #help : SSOAR_Bibtex(0: ref_id INT, 1: ref_bibtex TEXT, 2: author_name Text[], 3: year INT, 4: title Text, 5: others Text, 6: hash_value Text);
        #help : reference_ssoar_match(1: ref_id INT , 2: sowiport_ID INT, 3:Inter_match_ref INT );
        row_count = 0
        for row in rows:
            print "================"
            row_count += 1
            input_dict['ID'] = row[0]
            input_dict['author'] = row[2]
            input_dict['year'] = row[3]
            input_dict['title'] = row[4]
            print input_dict
            try:
                bdresult,param = bd.main(input_dict)
                Sowiport_id = bdresult['Sowiport_id']
                
                query = """INSERT INTO reference_ssoar_match(ref_id , sowiport_ID , Inter_match_ref ) VALUES (%s, %s, %s)"""
                data = ((input_dict['ID']), Sowiport_id, 0)
                
                cur.execute(query, data)
                conn.commit()
                print "row: %s -- ref_id: %s -- Inserted Successfully" % (row_count, row[0])

            except Exception as e:
                Error = str(e.args)
                logf.write("Error: {0}, {1}, ref_id {2},{3} \n".format(str(datetime.datetime.now().date()),str(datetime.datetime.now().time()),str(row[0]), Error))
                print(e.args)
        if conn is not None:
            conn.close()
        print '######################### End ###################################'
        t1 = time.time()
        total = t1 - t0
        print(total)
    except Exception as e:
        print(e)
if __name__ == '__main__':
    main()
