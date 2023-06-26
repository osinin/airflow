import logging
import pandas as pd
import numpy as np
import pendulum
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import get_current_context

from common.ch import ClickhouseClient
from notification.slack import slack_fail_alert

# logging
logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s')
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

# CH connections
ch_client = ClickhouseClient(Variable.get("ch_default_host"),
                             Variable.get("ch_default_port"),
                             Variable.get("ch_default_user"),
                             Variable.get("ch_default_password"),
                             int(Variable.get("ch_default_connect_timeout"))
                             )
base_url = 'https://api.huntflow.ru/v2'


def truncate_table(db: str, table_name: str) -> None:
    """
    This function needed to overwrite existing tables
    """

    query = f"TRUNCATE TABLE IF EXISTS {db}.{table_name} ON CLUSTER cluster"
    ch_client.execute_query(db, query)


def load_to_ch(db:str, table_name: str, df: pd.DataFrame) -> None:
    """
    This function inserts pandas dataframe into Clickhouse
    If it is not empty.
    """

    if not df.empty:
        ch_client.insert_pandas_df_nullable(db, table_name, df)
        log.info(f"Data inserted into: {db}.{table_name}, rows: {len(df)}")
    else:
        log.info(f"No data")


def get_new_tokens() -> None:
    """
    This function checks the expired access token or not.
    If expired, it refreshes pair of tokens.
    """

    access_token = Variable.get("huntflow_access_token")
    headers = {'Authorization': 'Bearer ' + f'{access_token}',
               'Content-Type': 'application/json'
               }
    acc_response = requests.get(f"{base_url}/accounts", headers=headers)
    if acc_response.status_code == 401 and acc_response.json()['errors'][0]['detail'] == 'token_expired':
        refresh_url = 'https://api.huntflow.ru/v2/token/refresh'
        refresh_token = Variable.get("huntflow_refresh_token")
        params = {
            'refresh_token': f'{refresh_token}',
            'Content-Type': 'application/json'
        }
        refresh_response = requests.post(refresh_url, json=params)
        Variable.set("huntflow_access_token", refresh_response.json()['access_token'])
        Variable.set("huntflow_refresh_token", refresh_response.json()['refresh_token'])
        log.info(f"Tokens updated")
    elif acc_response.status_code == 200:
        log.info(f"Tokens are not expired")
    else:
        log.info("Unexpected error")


def get_vacancies(ds: str) -> pd.DataFrame:
    """
    This function requests the number of pages with vacancies
    and gets all of them via cycle.
    """

    access_token = Variable.get("huntflow_access_token")
    headers = {'Authorization': 'Bearer ' + f'{access_token}',
               'Content-Type': 'application/json'
               }
    params = {'count': 100}
    response = requests.get(f"{base_url}/accounts/152555/vacancies", params=params, headers=headers)
    response_list = []
    for page in range(1, response.json()['total_pages'] + 1):
        params = {'page': str(page),
                  'count': 100}
        response = requests.get(f"{base_url}/accounts/152555/vacancies",
                                params=params,
                                headers=headers
                                )
        response_list += response.json()['items']
        log.info(f"page {page} DONE!")
    log.info('got all pages')
    result_df = pd.DataFrame(response_list)
    result_df['created'] = result_df['created'].str[:-6]
    result_df = result_df.rename(columns={'id': 'vacancy_id',
                                          'created': 'created_msk',
                                          'money': 'salary',
                                          'parent': 'parent_id',
                                          'state': 'vacancy_state'})
    columns = ['vacancy_id', 'account_division', 'position', 'company', 'salary', 'priority', 'vacancy_state',
               'created_msk', 'multiple', 'parent_id', 'account_vacancy_status_group', 'head']
    result_df = result_df[columns]
    result_df = result_df[result_df['created_msk'] < ds]
    log.info("Got vacancies df")
    # push list of vacancies
    vacancy_list = list(result_df['vacancy_id'].tolist())
    ti = get_current_context()['ti']
    ti.xcom_push(key='list_of_vacancies', value=vacancy_list)
    log.info(f"ds: {ds}")
    return result_df


def load_vacancy(**kwargs) -> None:
    schema = kwargs["schema"]
    table_name = kwargs["table_name"]
    ds = kwargs["ds"]
    vs_df = get_vacancies(ds)
    truncate_table(schema, table_name)
    load_to_ch(schema, table_name, vs_df)
    log.info(f"Vacancies loaded to CH")


default_args = {
    'retries': 0,
    'provide_context': True,
    'on_failure_callback': slack_fail_alert
}

with DAG(
        'Huntflow_to_CH_ETL',
        description='Huntflow ETL',
        default_args=default_args,
        start_date=pendulum.datetime(2023, 3, 1, tz="UTC"),
        schedule_interval='0 2 * * *',  # every day at 7 am (Tashkent)
        max_active_runs=1,
        catchup=False,
        tags=['osinin', 'DE-3', 'Huntflow', 'CH']
) as dag:
    refresh_tokens = PythonOperator(
        task_id='refresh_tokens',
        python_callable=get_new_tokens,
        dag=dag
    )

    vacancies = PythonOperator(
        task_id='load_vacancy',
        python_callable=load_vacancy,
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag,
        op_kwargs={"schema": "huntflow",
                   "table_name": "vacancies",
                   "ds": "{{ ds }}"}
    )

    refresh_tokens >> vacancies
