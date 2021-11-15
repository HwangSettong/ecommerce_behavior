# 개발환경
spark v3.2.0 사용  
언어 : python (평소 python을 자주 사용하며, spark sql 문법으로 spark를 이용해 본 경험이 있기 때문에 Pyspark를 이용하여 sql 쿼리문을 실행하기 위해 선정)  


# 요구사항
[https://www.kaggle.com/mkechinov/ecommerce-behavior-data-from-multi-category-store](https://www.kaggle.com/mkechinov/ecommerce-behavior-data-from-multi-category-store)  
2019-Nov.csv 파일만 활용  

1. 해당 전체 기간에서, KST 기준으로 active user 수가 제일 큰 날짜를 구하세요  
2. 1의 날짜에서, 가장 긴 세션 10개에 대해 "user_id, session_id, 세션시간"를 구하세요  
3. 1의 날짜의 15분단위로 active user 수를 구하세요  
4. 1의 날짜에서 view → cart → purchase 이벤트 진행에 따른 funnel 수치를 구하세요  

# 해결방법
2019-Nov.csv는 data라는 이름의 데이터프레임으로 생성하고 시작
```
data = spark.read.csv('2019-Nov.csv', header=True)
data.createOrReplaceTempView("data")
```

## 요구사항 1
**해당 전체 기간에서, KST 기준으로 active user 수가 제일 큰 날짜를 구하세요**  

1) event_type을 kst기준으로 변경. (spark 세션의 timezone이 kst로 세팅되어 있으므로 to_timestamp 이용)  
```
select to_timestamp(event_time) as kst_time, *
from data
```
> 세션의 timezone을 kst로 세팅하는 방법  
> ```
> spark = SparkSession.builder \
>     .appName("ecommerce behavior") \
>     .config('spark.sql.session.timeZone','UTC') \
>     .getOrCreate()
> ``` 

2) 1)의 결과에서 kst_time의 날짜를 그룹핑하고 (중복을 제거하여) user_id를 카운트하고, 카운트 수가 가장 큰 결과 1개만 출력.(중복 제거하여)  

**완성된 sql**
```
select to_date(kst_time) as kst_date, count(distinct(user_id)) as active_user
from (select to_timestamp(event_time) as kst_time, *
      from data)
group by kst_date
order by 2 desc limit 1
```


## 요구사항 2
**1의 날짜에서, 가장 긴 세션 10개에 대해 "user_id, session_id, 세션시간"를 구하세요.**  

1) 앞으로 문제에서 사용 될 1의 날짜만 있는 데이터프레임 df라는 이름으로 생성
```
# result1은 1의 날짜 (type(result1) : datetime)
add_day = (result1+ datetime.timedelta(days=1)).strftime("%Y-%m-%d") # '1의 날짜 + 1일' 을 string 타입으로 변환
day = result1.strftime("%Y-%m-%d")                                   # 1의 날짜 string 타입으로 변환

# day <= kst_time < add_day인 데이터 검색
query = '''select *
           from (select to_timestamp(event_time) as kst_time, *
                 from data)
           where (kst_time >= to_timestamp('{0}', 'yyyy-MM-dd')) and (kst_time < to_timestamp('{1}', 'yyyy-MM-dd'))'''.format(day,add_day)

# df 데이터프레임 생성
spark.sql(query).createOrReplaceTempView("df")
```

2) user_session 별로 시간이 max인것과 min인 것을 추출 (df1으로 명시)
3) user_session 과 user_id를 중복을 제거하여 추출 (df2로 명시)
4) df1과 df2를 user_session을 기준으로 inner join
5) join된 결과에서 user_id, user_session, max_time과 min_time의 차이(session_time)를 출력하는데 session_time이 가장 큰 것 10개만 추출  

**완성된 sql**
```
select user_id, df1.user_session as session_id, (df1.max_time-df1.min_time) as session_time
from (select user_session, max(kst_time) as max_time, min(kst_time) as min_time
      from df
      group by user_session) as df1
     INNER JOIN
     (select distinct user_id, user_session
      from df) as df2
     ON df1.user_session = df2.user_session
order by session_time desc limit 10
```


## 요구사항 3
**1의 날짜의 15분단위로 active user 수를 구하세요**  

1) df에서 시간의 분을 15분 단위로 끊어 case문을 작성.
2) df에서 user_id와 위의 case문의 결과(cut으로 명시)를 추출
3) 추출된 결과를 cut기준으로 그룹핑하고 중복을 제거하여 user_id 수를 카운트 함.  

**완성된 sql**
```
select cut||'~' as 15m_cut , count(distinct(user_id)) as active_user
from (select user_id, case when minute(kst_time) < 15 then  date_trunc('HOUR',kst_time)
                           when minute(kst_time) < 30 then  date_trunc('HOUR',kst_time)+INTERVAL 15 minutes
                           when minute(kst_time) < 45 then  date_trunc('HOUR',kst_time)+INTERVAL 30 minutes
                           else date_trunc('HOUR',kst_time)+INTERVAL 45 minutes end as cut
      from df)
group by 15m_cut
order by 15m_cut
```

## 요구사항 4
**1의 날짜에서 view → cart → purchase 이벤트 진행에 따른 funnel 수치를 구하세요**  
> view → cart → purchase 이벤트 진행에 따른 결과를 요구하므로 위에 진행에 맞지 않는것은 고려하지 않음  
> 즉, view가 있는 데이터들을 기준으로 cart와 purchase의 순서가 잘 지켜진 데이터만 추출함.  
> (세션 별로 이벤트가 view → purchase 로 바로 이어지는 경우, view 없이 cart → purchase 로 이어지는 등의 경우, cart와 perchase의 단독 이벤트만 존재하는 경우는 제외)  

1) df에서 user_settion별로 event_type을 나열. (아래의 예시와 같은 결과가 나오도록)  

|user_session|event_list|
|:--:|:--:|
|a7c5906e-5dd8-4175-aeca-eb5615844e67|view, view, cart, view, purchase, ...|
|...|...|
```
select user_session, concat_ws(', ',collect_list(event_type)) as event_list
from df
group by user_session
```

2) 1의 결과에서 event_list에 '%view%'가 존재하는 경우, (2) '%view%cart%'가 존재하는 경우, (3) '%view%cart%purchase'가 존재하는 경우를 각각 count함.
3) 2의 결과에서 각각의 count 수와 '%view%'가 존재하는 경우의 수를 기준으로 백분율 출력.  

**완성된 sql**
```
select view||" (100%)" as view, cart||" ("||round(100*cart/view,2)||"%)" as cart, purchase||" ("||round(100*purchase/view,2)||"%)" as purchase
from (select sum(int(event_list like '%view%')) as view, sum(int(event_list like '%view%cart%')) as cart, sum(int(event_list like '%view%cart%purchase%')) as purchase  
      from (select user_session, concat_ws(', ',collect_list(event_type)) as event_list
            from df
            group by user_session))'''
```
