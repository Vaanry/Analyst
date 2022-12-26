import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

default_args = {
    'owner': 'owner',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 21),
    'schedule_interval': '0 12 * * *'
}

year = 1994 + hash(f'{"ju-kolesnikova"}') % 23

@dag(default_args=default_args)
def owner():

# Загружаем данные

    @task()
    def get_data():
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv').query('Year == @year')
        return df
    
# Какая игра была самой продаваемой в этом году во всем мире?

    @task()
    def get_best_sales():
        max_sale = df.Global_Sales.max()
        best_sale = df.query('Global_Sales == @max_sale').Name.values[0]
        return best_sale
    
# Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько

    @task()
    def get_europe_games():
        europe_max = df.EU_Sales.max()
        best_genre = df.query('EU_Sales == @europe_max').Genre.to_list()
        return best_genre
    
# На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
#Перечислить все, если их несколько

    @task()
    def get_million_NA():
        NA_mill = df.query('NA_Sales > 1').groupby('Platform', as_index = False).agg({'Name':'count'})
        NA_max = NA_mill.Name.max()
        NA_platform = NA_mill.query('Name == @NA_max').Platform.to_list()
        return NA_platform
    
# У какого издателя самые высокие средние продажи в Японии?

    @task()
    def get_jap_avg():
        jap = df.groupby('Publisher', as_index = False).agg({'JP_Sales' : 'mean'}).sort_values('JP_Sales', ascending = False)
        jap_max = jap.JP_Sales.max()
        jap_publ = jap.query('JP_Sales == @jap_max').Publisher.to_list()
        return jap_publ
    
# Сколько игр продались лучше в Европе, чем в Японии?

    @task()
    def get_er_jap():
        er_jap = df.groupby('Name',as_index=False).agg({'EU_Sales':sum, 'JP_Sales':sum})
        best_eur = er_jap.query('EU_Sales > JP_Sales').shape[0]
        return best_eur 
    
# Вывод результатов

    @task()
    def print_data(best_sales, europe_games, million_NA, jap_avg, er_jap):
        print(f'Самая продаваемая игра в мире в {year} году: {best_sales}')
        print(f'Лучший жанр в Европе в {year} году: {europe_games}')
        print(f'Топ платформ в Северной Америке в {year} году: {million_NA}')
        print(f'Лучший издатель в Японии в {year} году: {jap_avg}')
        print(f'Игры, которые в Европе продавались лучше, чем в Японии, в {year} году: {er_jap}')

    data = get_data()
    best_sales = get_best_sales(data)
    europe_games = get_europe_games(data)
    million_NA = get_million_NA(data)
    jap_avg = get_jap_avg(data)
    er_jap = get_er_jap(data)   
    print_data(best_sales, europe_games, million_NA, jap_avg, er_jap)
    
owner = owner()