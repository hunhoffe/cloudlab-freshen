import json
from os.path import isfile, join
from cloudant.client import CouchDB
import time
import cv2
from numpy import asarray
import numpy as np

K_DBNAME = "frshimg"
K_DB2NAME = "resizeimg"
K_PREFIX = "resized-{}"
K_FILE_TYPE = "image/png"


def write2db(fn, dbname, user, passwd, url, img):
    ret_string = "FAILED: did not write {} to {}".format(fn, dbname)
    db_client = CouchDB(user, passwd, url=url, connect=True)
    if not db_client.session()['ok']:
        return ("cannot open database with {}:{},@{}".format(user, passwd, url))

    db_inst = db_client[dbname]
    id = int(time.time() * 1000)
    dta = {
        '_id': "{}".format(id),
        'name': fn
    }
    doc = db_inst.create_document(dta)
    f_dta = bytearray(img)
    ret = doc.put_attachment(fn, K_FILE_TYPE, f_dta)
    doc.save()
    db_client.disconnect()


def get_fn(inp):
    for i in inp:
        return i


def write_file(pth, fn, dta):
    fullpth = join(pth, fn)
    try:
        fh = open(fullpth, 'wb')
    except Exception as e:
        return e

    fh.write(dta)
    fh.close()
    return ("wrote: {}".format(fullpth))


def preprocess_image(im_arr,time_dict):
    start = time.time()
    im_arr = im_arr +b'\x00\x00\x00\x00\xae'  # round the buffer to a multiple of 32
    img_array = np.frombuffer(im_arr, np.float32)
    end = time.time() - start
    time_dict["array of img"] = (start, end)
    start = time.time()
    img2 = cv2.resize(img_array, None,  fx=0.5, fy=0.5, interpolation=cv2.INTER_AREA)
    end = time.time() - start
    time_dict["cv2.resize"] = (start, end)
    return img2


def test_count(user, passwd, url, key_idx, db1, db2):
    time_dict = {}
    start = time.time()
    db_client = CouchDB(user, passwd, url=url, connect=True)
    end = time.time() - start
    time_dict["DB_ClientConnect"] = (start, end)
    start = time.time()
    db_inst = db_client[db1]
    end = time.time() - start
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
    prog_img = preprocess_image(img, time_dict)
    end = time.time() - start_1

    time_dict["preprocess_image total"] = (start_1, end)
    start_1 = time.time()
    write2db(img_name, db2, user, passwd, url, prog_img)
    end = time.time() - start_1
    time_dict["write2db"] = (start_1, end)
    end = time.time() - start
    time_dict["total single proc time"] = (start, end)
    return time_dict


def main(args):
    user = args.get("user", "admin")  # = admin
    passwd = args.get("passwd",
                      "none")  # = $(kubectl get secret djb-couch-couchdb -o go-template='{{ .data.adminPassword }}' | base64 --decode)
    url = args.get("url", "none")  # = http://ipaddress_of_server:5984
    db1 = args.get("db1", K_DBNAME)
    db2 = args.get("db2", K_DB2NAME)
    count = args.get("count", 1)
    print("THIS IS IS\n")
    recval = test_count(user, passwd, url, count, db1, db2)
    return {
        "statusCode": 0,
        "body": json.dumps(({
            "label": recval,
        })),
    }

#if __name__ == "__main__":
#    test_count("admin","SO8J3E7p1fblnTVR3M2h","http://10.102.18.189:5984/",1, K_DBNAME,K_DB2NAME)

