from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime

default_args = {
    'owner':'sborisv74',
    'depends_on_past':False,
    'start_date':datetime(2019, 04, 01),
    'retries':0
    }

dag = DAG('dag_sborisv74_miniproject',
        default_args = default_args,
        catchup=False,
        schedule_interval='00 12 * * 1') # 1 means Monday

def send_report_to_vk():
    import pandas as pd
    import numpy as np
    import vk_api
    import random

    # Reading data
    path = 'https://raw.githubusercontent.com/sborisv74/Airflow_tests/refs/heads/main/ads_data_121288.csv'
    ads = pd.read_csv(path, parse_dates=[0])
    print('Data has readed')
    
    # Counting VIEWS and CLICKS
    views_count = ads \
            .query('event == "view"') \
            .groupby('date') \
            .agg(views=('event', 'count'))

    clicks_count = ads \
            .query('event == "click"') \
            .groupby('date') \
            .agg(clicks=('event','count'))

    # CTR
    ads_views_clicks = pd.merge(views_count, clicks_count, on='date').reset_index()
    ads_views_clicks['CTR'] = ads_views_clicks['clicks'] / ads_views_clicks['views'] * 100

    # сумма потраченных денег
    ads_views_clicks['money_views'] = ads_views_clicks.views * ((ads.ad_cost / 1000).unique()[0])

    # Prepare data for report
    money_0104 = float(ads_views_clicks[ads_views_clicks['date'] == '2019-04-01']['money_views'])
    views_0104 = float(ads_views_clicks[ads_views_clicks['date'] == '2019-04-01']['views'])
    clicks_0104 = float(ads_views_clicks[ads_views_clicks['date'] == '2019-04-01']['clicks'])
    CTR_0104 = float(ads_views_clicks[ads_views_clicks['date'] == '2019-04-01']['CTR'])
    
    money_0204 = float(ads_views_clicks[ads_views_clicks['date'] == '2019-04-02']['money_views'])
    views_0204 = float(ads_views_clicks[ads_views_clicks['date'] == '2019-04-02']['views'])
    clicks_0204 = float(ads_views_clicks[ads_views_clicks['date'] == '2019-04-02']['clicks'])
    CTR_0204 = float(ads_views_clicks[ads_views_clicks['date'] == '2019-04-02']['CTR'])
    print('All metrics counted')
    
    # Compose the Message
    message_vk = f'''Отчет по объявлению 121288 за 2 апреля:\n
    Траты: {round(money_0204, 2)} ({round((money_0204 - money_0104)/money_0104 * 100)} %)
    Показы: {views_0204} ({round((views_0204 - views_0104)/views_0104 * 100)} %)
    Клики: {clicks_0204} ({round((clicks_0204 - clicks_0104)/clicks_0104 * 100)} %)
    CTR: {round(CTR_0204, 2)} ({round((CTR_0204 - CTR_0104)/CTR_0104 * 100)} %)
    '''
    print('Report created')
    
    # Write Message to the file
    with open(f'report_2019-04-02.txt', 'w', encoding='utf-8') as fwr:
    fwr.write(message_vk)
    print('File was written')
    
    # Sending to VK
    app_token = ''
    chat_id = 1
    my_id = 12345
    vk_session = vk.api.VKApi(token=app_token)
    vk = vk_session.get_aip()

    vk.message.send(
        chat=chat_id,
        random_id=random.ranint(1, 2 ** 31),
        message=message_vk
        )
    print('Report sent')

    t1 = PythonOperator(task_id='ads_report',
                       python_callable=send_report_to_vk,
                       dag=dag
                       )
    