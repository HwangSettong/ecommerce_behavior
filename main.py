from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("ecommerce behavior") \
    .config('spark.sql.session.timeZone','Asia/Seoul') \
    .getOrCreate()

# csv 파일 로드
data = spark.read.csv('/Users/settong/dev/ecommerce_behavior/2019-Nov.csv', header=True)
data.createOrReplaceTempView("data")



###################################################################
# 1. 해당 전체 기간에서, KST기준으로 acitve user 수가 제일 큰 날짜를 구하세요. #
###################################################################
query = '''select to_date(kst_time) as kst_date, count(distinct(user_id)) as active_user
           from (select to_timestamp(event_time) as kst_time, *
                 from data)
           group by kst_date
           order by 2 desc limit 1'''

result1 = spark.sql(query).toPandas().iloc[0,0]
print("1. 해당 전체 기간에서, KST기준으로 acitve user 수가 제일 큰 날짜를 구하세요.")
print(result1)



###########################################################################
# 2. 1의 날짜에서, 가장 긴 세션 10개에 대해 "user_id, session_id, 세션시간"를 구하세요 #
###########################################################################

import datetime

add_day = (result1+ datetime.timedelta(days=1))#.strftime("%Y-%m-%d")
day = result1#.strftime("%Y-%m-%d")
query = '''select *
           from (select to_timestamp(event_time) as kst_time, *
                 from data)
           where (kst_time >= to_timestamp('{0}', 'yyyy-MM-dd')) and (kst_time < to_timestamp('{1}', 'yyyy-MM-dd'))'''.format(day,add_day)

spark.sql(query).createOrReplaceTempView("df")

query = '''select user_id, df1.user_session as session_id, (df1.max_time-df1.min_time) as session_time
           from (select user_session, max(kst_time) as max_time, min(kst_time) as min_time
                 from df
                 group by user_session) as df1
                INNER JOIN
                (select distinct user_id, user_session
                 from df) as df2
                ON df1.user_session = df2.user_session
           order by session_time desc limit 10'''

result2 = spark.sql(query)
print('\n\n\n2. 1의 날짜에서, 가장 긴 세션 10개에 대해 "user_id, session_id, 세션시간"를 구하세요')
result2.show(truncate=False)



##############################################
# 3. 1의 날짜의 15분단위로 active user 수를 구하세요 #
##############################################

query = '''select cut||'~' as 15m_cut , count(distinct(user_id)) as active_user
           from (select user_id, case when minute(kst_time) < 15 then  date_trunc('HOUR',kst_time)
                                      when minute(kst_time) < 30 then  date_trunc('HOUR',kst_time)+INTERVAL 15 minutes
                                      when minute(kst_time) < 45 then  date_trunc('HOUR',kst_time)+INTERVAL 30 minutes
                                      else date_trunc('HOUR',kst_time)+INTERVAL 45 minutes end as cut
                 from df)
           group by 15m_cut
           order by 15m_cut'''

result3 = spark.sql(query)
print("\n\n3. 1의 날짜의 15분단위로 active user 수를 구하세요")
result3.show(result3.count())



#########################################################################
# 4. 1의 날짜에서 view → cart → purchase 이벤트 진행에 따른 funnel 수치를 구하세요 #
#########################################################################

query = '''select view||" (100%)" as view, cart||" ("||round(100*cart/view,2)||"%)" as cart, purchase||" ("||round(100*purchase/view,2)||"%)" as purchase
           from (select sum(int(event_list like '%view%')) as view, sum(int(event_list like '%view%cart%')) as cart, sum(int(event_list like '%view%cart%purchase%')) as purchase  
                 from (select user_session, concat_ws(', ',collect_list(event_type)) as event_list
                       from df
                       group by user_session))'''

result4 = spark.sql(query)
print("\n\n4. 1의 날짜에서 view → cart → purchase 이벤트 진행에 따른 funnel 수치를 구하세요")
result4.show()
