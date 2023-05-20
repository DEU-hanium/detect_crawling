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
                    "gt":"now-20m"
                }
            }
        },
        "size": 10000
    }
    
    ban_list = []

    r = requests.get(url, auth=awsauth, json=query, headers=headers)
    
    r = r.json()
    hits_list = r["hits"]["hits"]
    
    ip_dic = {}
    
    for element in hits_list:
        if ip_dic.get(element["_source"]["client_ip"]) == None:
            ip_dic[element["_source"]["client_ip"]] = [element]
        else:
            ip_dic[element["_source"]["client_ip"]].append(element)
            
    for element in ip_dic:
        if len(ip_dic.get(element)) >= 10:
            ban_list.append({"ip":element, "memo": 1})
            continue
        
        for index, item in enumerate(ip_dic.get(element)):
            if item["_source"]["user_agent"] == "-":
                ban_list.append({"ip": element, "memo": 2})
                break
            else:
                if index == 0:
                    tempUserAgent = item["_source"]["user_agent"]
                else:
                    if tempUserAgent != item["_source"]["user_agent"]:
                        ban_list.append({"ip": element, "memo": 3})
                        break
        
            
    if len(ban_list) >= 1:
        con = pymysql.connect(host=os.environ.get('DBHOST').strip(),
                      user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), db=os.environ.get('DB'), charset='utf8')
        cur = con.cursor()
        for element in ban_list:
            
            sql = f"select * from require_list where ip='{element['ip']}'"
            cur.execute(sql)
            rows = cur.fetchall()
            if len(rows) != 0:
                continue
            sql = f"select * from ban_list where ip='{element['ip']}'"
            cur.execute(sql)
            rows = cur.fetchall()
            if len(rows) != 0:
                continue
            sql = f"insert into ban_list values ('{element['ip']}','{element['memo']}', current_timestamp)"
            cur.execute(sql)
            data = {
                'ip': element["ip"] +'/32'
            }
            requests.post(os.environ.get('BACKEND')+"/lambda", json=data)
            
        con.commit()
        con.close()