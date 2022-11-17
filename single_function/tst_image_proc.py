import json
from timeit import default_timer as timer
from os.path import isfile, join
from cloudant.client import CouchDB
import time
import cv2
import os
from numpy import asarray

K_DBNAME = "frshimg"
K_DB2NAME = "resizeimg"
K_PREFIX = "resized-{}"
K_FILE_TYPE="image/png"

def write2db(fn, dbname, user, passwd, url,img):
    db_client = CouchDB(user,passwd, url=url, connect=True)
    if not db_client.session()['ok']:
        return ("cannot open database with {}:{},@{}".format(user, passwd, url))

    db_inst = db_client[dbname]
    id = int(time.time()*1000)
    dta = {
        '_id':"{}".format(id),
        'name':fn
        }
    doc = db_inst.create_document(dta)
    f_dta = img
    ret=doc.put_attachment(fn, K_FILE_TYPE,f_dta)
    doc.save()
    db_client.disconnect()




def get_fn(inp):
    for i in inp:
        return i

def write_file(pth,fn, dta):
    fullpth = join(pth,fn)
    try:
        fh = open(fullpth, 'wb')
    except Exception as e:
        return e

    fh.write(dta)
    fh.close()
    return ("wrote: {}".format(fullpth))

def preprocess_image(img_arr,time_dict):
    start = timer()
    img_array = asarray(img_arr)
    end = timer()-start
    time_dict["convert image to array"] = end
    start = timer()
    img2 = cv2.resize(img_array, None, fx=0.5, fy=0.5, interpolation=cv2.INTER_AREA)
    end = timer() - start
    time_dict["cv2.resize"] = end
    return img2

def test_count(user, passwd, url, num_recs,db1, db2):
    time_dict = {}
    start = timer()
    db_client = CouchDB(user, passwd, url=url, connect=True)
    end = timer()-start
    time_dict["DB_ClientConnect"]=end
    start = timer()
    db_inst = db_client[db1]
    end = timer()-start
    time_dict["db_client"] = end
    start = timer()
    rec_count = db_inst.doc_count()
    end = timer()-start
    time_dict["rec_count"]=end
    start = timer()
    keys = db_inst.keys(remote=True)
    end = timer()-start
    time_dict["keys"] = end
    byte_count = 0
    rec_total = 0
    start = timer()
    ctr = 0
    while ctr < num_recs:
        i = keys[ctr]
        start_1 = timer()
        doc = db_inst.get(i, remote=True)
        end = timer()-start_1

        time_dict["iteration {}: db_inst.get".format(ctr)] = end
        img_dict = doc['_attachments'].keys()
        img_name = get_fn(img_dict)

        start_1 = timer()
        img = doc.get_attachment(img_name)
        end = timer()-start_1

        time_dict["iteration {}: doc.get_attachment".format(ctr)] = end

        start_1 = timer()
        prog_img = preprocess_image(img,time_dict)
        end = timer()-start_1

        time_dict["iteration {}: preprocess_image total".format(ctr)] = end

        start_1 = timer
        ret = write2db(img_name,db2, user,passwd,url,prog_img)
        end = timer()-start_1
        time_dict["write2db err"]=ret
        time_dict["iteration {}: write2db".format(ctr)] = end
        ctr +=1
    end = timer()-start
    time_dict["total loop time"] = end
    return time_dict

def main(args):
    user = os.getenv('FRSH_USR')
    passwd = os.getenv('FRSH_PWD')
    pth = os.getenv('FRSH_FILE_PATH')
    url = os.getenv('FRSH_URL')

    url = args.get("url","none")  # = http://ipaddress_of_server:5984
    db1 = "frshimg"
    db2 = "resizeimg"
    count = "1"
    recval = test_count(user,passwd,url,count,db1, db2)
    return {
        "statusCode" :0,
        "body": json.dumps(({
            "label": recval,
        })),
    }


if __name__ == '__main__':
    main()
