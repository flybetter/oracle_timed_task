# *-* coding:utf8 *-*
import os
import urllib2

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
import cx_Oracle
import time
import pytz
from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler


def create_wechat_sql(yesterday_short):
    sql = "SELECT  T2.CITY_ID,T2.WECHATAVG,T2.WECHATSHARECOUNT,T2.WECHATSUMAVG FROM ( SELECT T1.*,ROWNUM RN FROM ( SELECT T3.*,T5.WECHATTOTALUSER FROM ( SELECT DIC.DIC_TEXT AS CITYNAME, T.CITY_ID, DIC.ORDER_NUM, ROUND(AVG(NVL(T.APPACTIVE,0)))AS APPACTIVE, SUM(NVL(T.CRMINC,0)) AS CRMINC, ROUND(AVG(NVL(T.WECHATAVG,0)))AS WECHATAVG, ROUND(AVG(NVL(T.WECHATSUMAVG,0)))AS WECHATSUMAVG, ROUND(AVG(NVL(T.WECHATSHARECOUNT,0)))AS WECHATSHARECOUNT FROM CO_DIC DIC LEFT JOIN DWH_AGGR_INDEX T ON DIC.DIC_VALUE = T.CITY_ID WHERE DIC.DIC_NAME = '城市' AND T.DATELINE BETWEEN '{}' AND '{}' GROUP BY DIC.DIC_TEXT, DIC.ORDER_NUM,T.CITY_ID ) T3, (SELECT * FROM ( SELECT ROW_NUMBER() OVER(PARTITION BY T4.CITY_ID ORDER BY T4.CITY_ID,T4.DATELINE DESC) RN, T4.CITY_ID, T4.WECHATTOTALUSER FROM DWH_AGGR_INDEX T4 WHERE T4.DATELINE BETWEEN '{}' AND '{}' ) WHERE RN =1) T5 WHERE T3.CITY_ID = T5.CITY_ID ORDER BY ORDER_NUM DESC ) T1 WHERE ROWNUM <= 10 ) T2 WHERE T2.RN >= 1 and T2.CITY_ID!=0".format(
        yesterday_short, yesterday_short, yesterday_short, yesterday_short)
    return sql


def create_update_sql(the_day_before_yesterday):
    sql = "update dwc_etl_dispatch_def set current_data_date='{}',exec_start_time=null,exec_end_time=null where disp_name like '%微信%'".format(
        the_day_before_yesterday)
    return sql


def create_syn_produce(yesterday_short):
    sql = "begin pkg_aggr_index.p_index_weChatAvg('{}'); end;".format(yesterday_short)
    return sql


def oracle_connect(sql, get_result=False):
    print(sql)
    conn = cx_Oracle.connect('app/app@202.102.74.64:1521/app')
    c = conn.cursor()
    x = c.execute(sql)

    flag_result = True
    if get_result:
        results = x.fetchall()
        print(results)
        for result in results:
            for item in result:
                if item == 0:
                    flag_result = False
                    break
    c.close()
    conn.commit()
    conn.close()
    return flag_result


def begin():
    timez = pytz.timezone('Asia/Shanghai')
    now = datetime.now(timez)
    yesterday = (now - timedelta(days=1)).strftime('%Y-%m-%d')
    print(yesterday)
    the_day_before_yesterday = (now - timedelta(days=2)).strftime('%Y%m%d')
    print(the_day_before_yesterday)
    yesterday_short = (now - timedelta(days=1)).strftime('%Y%m%d')
    print(yesterday_short)
    now = datetime.now(timez)
    print(now.strftime('%Y-%m-%d %H:%M:%S'))

    while not check_zero(yesterday_short):
        url_function()
        procedure(the_day_before_yesterday, yesterday_short)
        time.sleep(1 * 60 * 30)  # 30 min

    # sql = create_wechat_sql(yesterday_short);
    # flag = oracle_connect(sql, True)


def url_function():
    print('url_function')
    response = urllib2.urlopen("http://202.102.74.53/stat/jobs/count/weixin2.php")
    time.sleep(1 * 60)
    print response.read()


def check_zero(yesterday_short):
    print('start check_zero')
    sql = create_wechat_sql(yesterday_short);
    flag = oracle_connect(sql, True)
    return flag


def procedure(the_day_before_yesterday, yesterday_short):
    print('start procedure')
    sql = create_update_sql(the_day_before_yesterday);
    oracle_connect(sql)

    time.sleep(1 * 60 * 10)  # 10 min

    sql = create_syn_produce(yesterday_short);
    oracle_connect(sql)


if __name__ == '__main__':
    # 8点开始同步
    timez = pytz.timezone('Asia/Shanghai')
    scheduler = BlockingScheduler(timezone=timez)
    scheduler.add_executor('processpool')
    scheduler.add_job(begin, 'cron', hour=9, minute=30, second=00, misfire_grace_time=30)
    scheduler.start()

    # begin()
    # url_function()
    # timez_tmp = pytz.timezone('Asia/Shanghai')
    # now = datetime.now(timez_tmp)
    # print(int(now.strftime('%H')) > 17)
