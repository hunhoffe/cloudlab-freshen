helm repo add couchdb https://apache.github.io/couchdb-helm
helm install frsh-couch couchdb/couchdb  --set couchdbConfig.couchdb.uuid=$(curl https://www.uuidgenerator.net/api/version4 2>/dev/null | tr -d -)

export FRSH_IP=`kubectl get service/frsh-couch-svc-couchdb -o jsonpath='{.spec.clusterIP}'`
export FRSH_USR='admin'
export FRSH_PWD=$(kubectl get secret frsh-couch-couchdb -o go-template='{{ .data.adminPassword }}' | base64 --decode)
export FRSH_URL="http://${FRSH_IP}:5984/"
export FRSH_FILE_PATH="$PWD/xtra"
/home/djb/GRepository/fresh/new_fresh/cloudlab-freshen/single_function
#"ok":true}
curl -X GET http://admin:"$FRSH_PWD"@"$FRSH_IP":5984/_all_dbs
#["_replicator","_users"]
pip3 install cloudant
tar -xvf xtra.tar
python3 load_coachdb.py #repeat if failure occurs

wsk -i action create procit --docker st00p1d/action-python-v3.6-ai:latest proc_couchdb.py 
wsk -i action invoke --result procit --param url $FRSH_URL --param passwd $FRSH_PWD 
#{ "body": "{"label": {"recs_indb": 101, "recs_processed": 101, "bytes_read": 14270647, "elapsed_time": 2.9134957709975424}}", "statusCode": 200 }
#NOTE THE INITIAL INVOCATION MIGHT NOT PRESENT A RESPONSE
wsk -i action create timeit --docker st00p1d/action-python-v3.6-ai:latest  image_proc3.py 

wsk -i action invoke timeit --result --param url $FRSH_URL --param passwd $FRSH_PWD --param count 12

wsk -i action create timesingle --docker st00p1d/action-python-v3.6-ai:latest  image_proc4.py 

wsk -i action invoke timesingle --result --param url $FRSH_URL --param passwd $FRSH_PWD --param count 1


#to reset the db
curl -X DELETE http://admin:"$FRSH_PWD"@"$FRSH_IP":5984/resizeimg
python3 init_resize.py
