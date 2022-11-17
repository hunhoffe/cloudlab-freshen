import sys
import os

from timeit import default_timer as timer
import json

from cloudant.client import CouchDB

K_DBNAME = "frshimg"


def get_fn(inp):
    for i in inp:
        return i




def get_records(user, passwd, url,dbname):
    db_client = CouchDB(user, passwd, url=url, connect=True);
    db_inst = db_client[dbname]
    rec_count = db_inst.doc_count()
    # print('The database contains {} records'.format(rec_count))
    keys = db_inst.keys(remote=True)
    # print('we have the following keys {} '.format(keys))
    byte_count = 0
    rec_total = 0
    start = timer()
    for i in keys:
        doc = db_inst.get(i, remote=True)
        img_dict = doc['_attachments'].keys()
        img_name = get_fn(img_dict)
        img = doc.get_attachment(img_name)
        byte_count += len(img)
        rec_total += 1
    end = timer() - start

    status = {"dbname":dbname, "recs_indb": rec_count, "recs_processed":rec_total, "bytes_read": byte_count, "elapsed_time": end}
    return {
        "statusCode": 200,
        "body": json.dumps(({
            "label": status,
        })),
    }


def main(args):
    user = args.get("user","admin")
    passwd = args.get("passwd", "none")
    url = args.get("url", "none")
    dbname = args.get("dbname", K_DBNAME)
    recval = get_records(user, passwd, url,dbname)
    return (recval)

