import boto3
import requests
from requests_aws4auth import AWS4Auth
import pymysql
import os
from datetime import datetime
import json

region = 'ap-northeast-2'
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

host = os.environ.get('OPENSEARCH')


def lambda_handler(event, context):
    index = 'logs'
    url = host + '/' + index + '/_search'
    headers = { "Content-Type": "application/json" }
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
    
    # 모든 트래픽 가져오기
    hits_list = r["hits"]["hits"]
    
    ip_dic = {}
    
    # 크롤링 트래픽을 판별하기 전 ip별로 그룹화
    for element in hits_list:
        if ip_dic.get(element["_source"]["client_ip"]) == None:
            ip_dic[element["_source"]["client_ip"]] = [element]
        else:
            ip_dic[element["_source"]["client_ip"]].append(element)
            
    # 크롤링 트래픽 판별
    for element in ip_dic:
        # 단순 테스트용 10회 이상 접속시 크롤링으로 판별
        if len(ip_dic.get(element)) >= 10:
            ban_list.append({"ip":element, "memo": 1})
            continue
        
        for index, item in enumerate(ip_dic.get(element)):
            # user_agent가 없는 경우 크롤링으로 판별 
            if item["_source"]["user_agent"] == "-":
                ban_list.append({"ip": element, "memo": 2})
                break
            else:
                if index == 0:
                    tempUserAgent = item["_source"]["user_agent"]
                else:
                    # user_agent가 이전의 트래픽과 다른 경우 크롤링으로 판별
                    if tempUserAgent != item["_source"]["user_agent"]:
                        ban_list.append({"ip": element, "memo": 3})
                        break
        
            
    if len(ban_list) >= 1:
        con = pymysql.connect(host=os.environ.get('DBHOST').strip(),
                      user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), db=os.environ.get('DB'), charset='utf8')
        cur = con.cursor()
        for element in ban_list:
            
            # 허용된 ip인지 확인
            sql = f"select * from require_list where ip='{element['ip']}'"
            cur.execute(sql)
            rows = cur.fetchall()
            if len(rows) != 0:
                continue

            # 이미 ban_list에 있는 ip인지 확인
            sql = f"select * from ban_list where ip='{element['ip']}'"
            cur.execute(sql)
            rows = cur.fetchall()
            if len(rows) != 0:
                continue

            # ban_list에 추가
            sql = f"insert into ban_list values ('{element['ip']}','{element['memo']}', current_timestamp)"
            cur.execute(sql)
            data = {
                'ip': element["ip"] +'/32'
            }

            # 백엔드 /lambda로 post 요청
            requests.post(os.environ.get('BACKEND')+"/lambda", json=data)
            
            # opensearch에 두 번째 인덱스에 요청하기 위해 url 재설정
            url = host + '/ban_list/_doc'
            now = datetime.now()
            
            # geoLocation
            request_url = 'https://geolocation-db.com/jsonp/' + element['ip']
            response = requests.get(request_url)
            result = response.content.decode()
            result = result.split("(")[1].strip(")")
            result  = json.loads(result)
            
            # opensearch에 ban_list 추가
            opensearchData = {
                'ip': element['ip'],
                'timestamp': now.strftime('%Y-%m-%dT%H:%M:%S'),
                'memo': element['memo'],
                'location': {
                    'lat': result['latitude'],
                    'lon': result['longitude']
                }
            }
            r = requests.post(url, auth=awsauth, json=opensearchData, headers=headers)
            print(r.text)
            
        con.commit()
        con.close()