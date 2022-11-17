import json
from os.path import isfile, join
from cloudant.client import CouchDB
import time
import cv2


K_DBNAME = "frshimg"
K_DB2NAME = "resizeimg"
K_PREFIX = "resized-{}"
K_FILE_TYPE="image/png"

def write2db(fn, dbname, user, passwd, url):
    ret_string = "FAILED: did not write {} to {}".format(fn, dbname)
    db_client = CouchDB(user,passwd, url=url, connect=True)
    if not db_client.session()['ok']:
        return ("cannot open database with {}:{},@{}".format(user, passwd, url))

    db_inst = db_client[dbname]
    resized_fn =K_PREFIX.format(fn)
    id = int(time.time()*1000)
    dta = {
        '_id':"{}".format(id),
        'name':fn
        }
    doc = db_inst.create_document(dta)
    fh = open(join('/tmp',resized_fn),'rb')
    if fh:
        f_dta = bytearray(fh.read())
        ret=doc.put_attachment(fn, K_FILE_TYPE,f_dta)
        doc.save()
        fh.close()
        ret_string = "OK: wrote {} bytes for {}".format(len(f_dta), id)
    db_client.disconnect()

    return ret_string



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

def preprocess_image(d_pth,fn,time_dict):
    start = time.time()
    img = cv2.imread(join(d_pth,fn))
    end = time.time()-start
    time_dict["cv2.imread {}".format(fn)] = (start, end)
    start = time.time()
    img2 = cv2.resize(img, None, fx=0.5, fy=0.5, interpolation=cv2.INTER_AREA)
    end = time.time() - start
    time_dict["cv2.resize"] = (start, end)
    xxx = join(d_pth,K_PREFIX.format(fn))
    start = time.time()
    cv2.imwrite(xxx,img2)
    end = time.time()-start
    time_dict["cv2.imwrite"] = (start, end)

def test_count(user, passwd, url,key_idx, db1, db2):
    time_dict = {}
    start = time.time()
    db_client = CouchDB(user, passwd, url=url, connect=True)
    end = time.time()-start
    time_dict["DB_ClientConnect"]=(start, end)
    start = time.time()
    db_inst = db_client[db1]
    end = time.time()-start
    time_dict["db_client"] = (start, end)

    byte_count = 0
    rec_total = 0
    start = time.time()

    i = str(key_idx)
    start_1 = time.time()
    doc = db_inst.get(i, remote=True)
    end = time.time() - start_1
    time_dict["db_inst.get idx {}".format(i)] = (start_1, end)
    img_dict = doc['_attachments'].keys()
    img_name = get_fn(img_dict)
    start_1 = time.time()
    img = doc.get_attachment(img_name)
    end = time.time() - start_1
    time_dict["doc.get_attachment"] = (start_1, end)

    start_1 = time.time()
    ret = write_file('/tmp', img_name, img)
    end = time.time() - start_1

    time_dict["write_file"] = (start_1, end)
    start_1 = time.time()
    preprocess_image('/tmp', img_name, time_dict)
    end = time.time() - start_1

    time_dict["preprocess_image total"] = (start_1, end)
    start_1 = time.time()
    ret = write2db(img_name, db2, user, passwd, url)
    end = time.time() - start_1
    time_dict["iteration {}: write2db"] = (start_1, end)
    end = time.time()-start
    time_dict["total single proc time"] = (start,end)
    return time_dict

def main(args):
    user = args.get("user","admin") # = admin
    passwd = args.get("passwd","none") # = $(kubectl get secret djb-couch-couchdb -o go-template='{{ .data.adminPassword }}' | base64 --decode)
    url = args.get("url","none")  # = http://ipaddress_of_server:5984
    db1 = args.get("db1",K_DBNAME)
    db2 = args.get("db2",K_DB2NAME)
    count = args.get("count",1)
    recval = test_count(user,passwd,url,count, db1, db2)
    return {
        "statusCode" :0,
        "body": json.dumps(({
            "label": recval,
        })),
    }
