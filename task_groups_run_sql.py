import logging
import os
import re
import pendulum

from datetime import timedelta

from airflow import DAG
from airflow.configuration import conf
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

from common.ch import ClickhouseClient
from notification.slack import slack_fail_alert

# logging
logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s')
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

ch_client = ClickhouseClient(Variable.get("ch_default_host"),
                             Variable.get("ch_default_port"),
                             Variable.get("ch_default_user"),
                             Variable.get("ch_default_password"),
                             int(Variable.get("ch_default_connect_timeout"))
                             )

scripts = ['finance_private_uzumbank_b2c_w_finance',
           'finance_private_uzumbank_b2b_w_finance',
           'finance_private_uzumbank_b2b_b2c_w_finance']


def insert_data_sandbox(**kwargs):
    file_name = kwargs["file_name"]
    table_name = file_name[16:]

    ch_client.execute("sandbox", f"drop table if exists sandbox.{table_name}")
    table_ddl = ch_client.get_ddl('finance_private', table_name)
    sandbox_table_ddl = table_ddl.replace("CREATE TABLE finance_private", "CREATE TABLE sandbox")
    sandbox_table_ddl = re.sub("ENGINE.+\n", "ENGINE = MergeTree \n", sandbox_table_ddl)
    log.info(sandbox_table_ddl)
    ch_client.execute("sandbox", sandbox_table_ddl)

    query = open(os.path.join(conf.get("core", "dags_folder"), "sql", "scripts", f"{file_name}.sql")).read()
    if ';' not in query:
        ch_client.execute('sandbox', query)
    else:  # разделяет один запрос на несколько и выполняет по очереди
        sub_queries = query.split(';')
        i = 0
        for sub_query in sub_queries:
            log.info(f'Executing sub_query #{i}. Text \n{sub_query}')
            ch_client.execute('sandbox', sub_query)
            i += 1


def insert_data_marts(**kwargs):
    table_name = kwargs["file_name"][16:]
    ch_client.execute_query('marts', f'TRUNCATE TABLE finance_private.{table_name}')
    ch_client.execute_query('marts', f'INSERT INTO finance_private.{table_name} SELECT * FROM sandbox.{table_name}')
    ch_client.execute_query('marts', f'DROP TABLE sandbox.{table_name}')


default_args = {
    'owner': 'ubank',
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
    'on_failure_callback': slack_fail_alert
}

with DAG(
        'finance_private__uzumbank_finance__dag',
        default_args=default_args,
        max_active_runs=1,
        max_active_tasks=1,
        tags=['osinin', 'FID-715', 'marts'],
        catchup=False,
        schedule_interval='15 3 * * *',  # every day at 8-15 am (Tashkent)
        start_date=pendulum.datetime(2022, 12, 31, tz="UTC")
) as dag:
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    groups = []
    for table in scripts:
        tg_id = f'{table}'
        with TaskGroup(group_id=tg_id) as tg:
            insert_data_sandbox_task = PythonOperator(
                task_id=f'insert_data_sandbox_{table}',
                python_callable=insert_data_sandbox,
                op_kwargs={"file_name": table}
            )

            insert_data_marts_task = PythonOperator(
                task_id=f"insert_data_marts_{table}",
                python_callable=insert_data_marts,
                op_kwargs={"file_name": table}
            )

            insert_data_sandbox_task >> insert_data_marts_task
            groups.append(tg)

    start >> groups >> finish