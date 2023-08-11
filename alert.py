import telegram
import io
import matplotlib.pyplot as plt
import seaborn as sns
import pandahouse as ph
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import secret


connection = {
    'host': secret.host,
    'password': secret.password,
    'user': secret.user,
    'database': secret.database
     }

def alert(data, test, metric):
    is_alert=False
    df = data[['hm', metric]].groupby('hm').describe()
    df.columns = df.columns.droplevel(0)
    df.reset_index(inplace=True)
    df['icr']=df['75%']-df['25%']
    df['top']=df['75%']+df['icr']*3
    df['bottom']=df['25%']-df['icr']*3
    current_time = test['hm'].iloc[-1]
    current_metric = test[metric].iloc[-1]
    if df[df['hm']==current_time]['top'].iloc[0]<current_metric or df[df['hm']==current_time]['bottom'].iloc[0]>current_metric:
        is_alert=True  #после тестирования знаки надо поменять на противоположные
    return is_alert, current_time, current_metric, df

def run_alert(chat=None):
    q1 = '''select 
    toDate(time) as date,
    formatDateTime(toStartOfFifteenMinutes(time), '%R') as hm,
    uniqExact(user_id) as unique_users,
    countIf(user_id, action='like') as likes,
    countIf(user_id, action='view') as views,
    countIf(user_id, action='like') / uniqExact(user_id) as lpu,
    countIf(user_id, action='view') / uniqExact(user_id) as vpu,
    100*likes/views as ctr,
    uniqExact(post_id) as posts,
    count(post_id) / uniqExact(user_id) as ppu
    from
    {db}.feed_actions
    where time >= today()-8
    and time < today()
    group by date, hm'''
    data1 = ph.read_clickhouse(q1, connection=connection)
    
    q2 = '''select 
    toDate(time) as date,
    formatDateTime(toStartOfFifteenMinutes(time), '%R') as hm,
    uniqExact(user_id) as unique_users,
    countIf(user_id, action='like') as likes,
    countIf(user_id, action='view') as views,
    countIf(user_id, action='like') / uniqExact(user_id) as lpu,
    countIf(user_id, action='view') / uniqExact(user_id) as vpu,
    100*likes/views as ctr,
    uniqExact(post_id) as posts,
    count(post_id) / uniqExact(user_id) as ppu
    from
    {db}.feed_actions
    where time >= today()-1
    and time <  toStartOfFifteenMinutes(now())
    group by date, hm'''
    test1 = ph.read_clickhouse(q2, connection=connection)
    
    
    q3 ='''select
    toDate(time) as date,
    formatDateTime(toStartOfFifteenMinutes(time), '%R') as hm,
    uniqExact(user_id) as users_msg,
    count(user_id) as messages,
    count(user_id) / uniqExact(user_id) as mpu
    from
    {db}.message_actions
    where time >= today()-8
    and time < today()
    group by date, hm'''
    
    data2 = ph.read_clickhouse(q3, connection=connection)
    
    q4 = '''select
    toDate(time) as date,
    formatDateTime(toStartOfFifteenMinutes(time), '%R') as hm,
    uniqExact(user_id) as users_msg,
    count(user_id) as messages,
    count(user_id) / uniqExact(user_id) as mpu
    from
    {db}.message_actions
    where time >= today()-1
    and time <  toStartOfFifteenMinutes(now())
    group by date, hm'''
    test2 = ph.read_clickhouse(q4, connection=connection)
    
    
    bases = [(data1,test1), (data2,test2)]
    metrics = {0:{'unique_users':'Unique users', 'likes':'Likes', 'views':'Views', 'lpu':'Likes per user', 
              'vpu': 'Views per user', 'ctr':'CTR', 'posts':'Posts', 'ppu':'Posts by user'},
            1:{'users_msg':'Users Messenger', 'messages':'Messages', 'mpu':'Messages per user'}}

    
    chat_id = chat or secret.chat_id
    bot = telegram.Bot(secret.token)
    for indx, data in enumerate(bases):
        for metric in metrics[indx].keys():

            is_alert, current_time, current_metric, df=alert(data[0], data[1], metric)
            if is_alert:
                msg = f'''Метрика <b>{metrics[indx][metric]}</b> в срезе {current_time}:
tекущее значение: <b>{current_metric:.2f}</b>;
oтклонение от предыдущего значения: <b>{(1-(current_metric/data[1][metric].iloc[-2])):.2f}</b>.'''

                sns.set(rc={'figure.figsize':(16, 10)})
                plt.tight_layout()
                ax = sns.lineplot(data=data[1], x=data[1]['hm'], y=data[1][metric], label=metrics[indx][metric])
                ax = sns.lineplot(data=df, x=df['hm'], y=df['top'], label='top')
                ax = sns.lineplot(data=df, x=df['hm'], y=df['bottom'], label='bottom')


                for index, label in enumerate(ax.get_xticklabels()):
                        if index%10==0:
                            label.set_visible(True)
                        else:
                            label.set_visible(False)
                ax.set(xlabel='time')
                ax.set(ylabel=metrics[indx][metric])

                ax.set_title(metrics[indx][metric])

                plot_object = io.BytesIO()
                ax.figure.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = f'{metrics[indx][metric]}.png'
                plt.close()

                bot.sendMessage(chat_id=chat_id, text=msg, parse_mode="html")
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)

default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 8, 9),
}
schedule_interval = '*/15 * * * *'


dag = DAG('alert', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='run_alert',
                    python_callable=run_alert,
                    dag=dag)


t1