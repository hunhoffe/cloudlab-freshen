from cloudant.client import CouchDB
import os
K_DBNAME="resizeimg"

def create_db(user,passwd, url, dbname):
    db_client = CouchDB(user, passwd, url=url, connect=True)
    if not db_client.session()['ok']:
        print(f'cannot open database with {user}:{passwd},@{url}')
        sys.exit(-1)

    frsh_db =db_client.create_database(dbname)
    if not frsh_db.exists():
        print("Cannot create database {}".format(dbname))
        sys.exit(-2)


    print("created {}".format(dbname))

if __name__ == "__main__":
    user = os.getenv('FRSH_USR')
    passwd = os.getenv('FRSH_PWD')
    url = os.getenv('FRSH_URL')
    pth = os.getenv('FRSH_FILE_PATH')
    create_db(user,passwd,url, K_DBNAME)
