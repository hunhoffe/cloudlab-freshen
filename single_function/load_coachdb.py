import sys
import os
from cloudant.client import CouchDB
from os.path import isfile,join

K_DBNAME="frshimg"
K_FILE_TYPE="image/png"
def process_files(user,passwd,url,pth):
    db_client = CouchDB(user,passwd,url=url,connect=True)
    if not db_client.session()['ok']:
        print(f'cannot open database with {user}:{passwd},@{url}')
        sys.exit(-1)
    # create image db
    frsh_db =db_client.create_database(K_DBNAME)
    if not frsh_db.exists():
        print("Cannot create database {}".format(K_DBNAME))
        sys.exit(-2)
    # get files
    file_list = [f for f in os.listdir(pth) if isfile(join(pth,f))]
    ctr =0
    for fn in file_list:
        dta ={
            '_id':"{}".format(ctr),
            'name':"frsh_img_{:05d}".format(ctr)
        }
        doc =frsh_db.create_document(dta)
        if not doc.exists():
            print("Could not create document {}".format(dta))
            sys.exit(-3)

        #get the file
        full_path=join(pth,fn)
        fh = open(full_path,'rb')
        if fh:
            f_dta = bytearray(fh.read())
            doc.put_attachment(fn,K_FILE_TYPE,f_dta)
            doc.save()
            print("added file {:05d} name {}".format(ctr,full_path))
            ctr +=1
        else:
            print("could not open {}".format(full_path))
            sys.exit()

    db_client.disconnect()



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    user = os.getenv('FRSH_USR') # = admin
    passwd = os.getenv('FRSH_PWD') # = $(kubectl get secret djb-couch-couchdb -o go-template='{{ .data.adminPassword }}' | base64 --decode)
    url = os.getenv('FRSH_URL')  # = http://ipaddress_of_server:5984
    pth = os.getenv('FRSH_FILE_PATH') # = file path
    process_files(user,passwd,url, pth)



