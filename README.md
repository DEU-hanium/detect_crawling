# 크롤링 트래픽 탐지 알고리즘
## RDS를 이용한 데이터베이스 생성 
>mysql 사용<br>
>![image](https://github.com/DEU-hanium/detect_crawling/assets/113816822/211730a8-036a-406d-a8cc-66a0ed5d4cf2)<br>
>3306포트 사용 <br>
>![image](https://github.com/DEU-hanium/detect_crawling/assets/113816822/2f9b5601-ee8d-4c8f-a4df-6accbd562d05))<br>
## TABLE 생성 
>CREATE TABLE ban_list ( <br>
  ip varchar(20) PRIMARY KEY,<br>
  memo varchar(20),<br>
  created_at timestamp DEFAULT CURRENT_TIMESTAMP<br>
);
<br>
>CREATE TABLE require_list (
    ip varchar(20) PRIMARY KEY,
    memo varchar(20),
    created_at timestamp DEFAULT CURRENT_TIMESTAMP
);
## Lambda를 이용해 opensearch-to-lambda
>lambda 생성 
![image](https://github.com/DEU-hanium/detect_crawling/assets/113816822/244e725e-a6e1-43c4-9aed-588993af702e)
>트리거 추가
>![image](https://github.com/DEU-hanium/detect_crawling/assets/113816822/82539bd8-1255-46bf-8a7d-62429931e8d8)

