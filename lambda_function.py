import boto3
import requests
from requests_aws4auth import AWS4Auth
import pymysql
import os

region = 'ap-northeast-2'
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

host = os.environ.get('OPENSEARCH')
index = 'logs'
url = host + '/' + index + '/_search'

headers = { "Content-Type": "application/json" }

def lambda_handler(event, context):
    query = {
        "query": {
            "range": {
                "timestamp": {
                    "gt":"now-60m"
                }
            }
        },
        "size": 10000
    }
    
    ban_list = []

    r = requests.get(url, auth=awsauth, json=query, headers=headers)
    
    r = r.json()
    print(r)
    hits_list = r["hits"]["hits"]
    
    ip_dic = {}
    
    for element in hits_list:
        if ip_dic.get(element["_source"]["client_ip"]) == None:
            ip_dic[element["_source"]["client_ip"]] = [element]
        else:
            ip_dic[element["_source"]["client_ip"]].append(element)
            
    for element in ip_dic:
        if len(ip_dic.get(element)) >= 5:
            ban_list.append(element)
            
    if len(ban_list) >= 1:
        con = pymysql.connect(host=os.environ.get('DBHOST').strip(), 
                      user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), db=os.environ.get('DB'), charset='utf8')
        cur = con.cursor()
        for element in ban_list:
            
            sql = "select * from require_list where ip='"+ element +"'"
            cur.execute(sql)
            rows = cur.fetchall()
            if len(rows) != 0:
                continue
            sql = "select * from ban_list where ip='"+ element +"'"
            cur.execute(sql)
            rows = cur.fetchall()
            if len(rows) != 0:
                continue
            sql = "insert into ban_list values ('"+ element +"', '1', current_timestamp)"
            cur.execute(sql)
            data = {
                'ip': element+'/32'
            }
            print(data)
            requests.post(os.environ.get('BACKEND')+"/lambda", json=data)
            
        con.commit()
        con.close()