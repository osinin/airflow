import pendulum
from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task_sensor import ExternalTaskSensor

default_args = {
    'owner': 'xxx',
    'retries': 1,
    'provide_context': True,
    #'on_failure_callback': slack_fail_alert
}

with DAG(
        'raw__ecosystem_identifiers_days__etl',
        description='ecosystem identifiers days',
        default_args=default_args,
        start_date=pendulum.datetime(2022, 12, 31, tz="UTC"),
        schedule_interval='0 3 * * *',
        max_active_runs=1,
        catchup=False,
        tags=['osinin']
) as dag:

    wait_for_marts = ExternalTaskSensor(
        task_id='wait_for_marts',
        external_dag_id='marts__s3__market__extraction',
        external_task_id='finish',
        execution_delta=timedelta(hours=2),
        mode='reschedule',                   # use poke if the check interval  is cleared in seconds
        poke_interval=900,                   # check interval
        timeout=10800,                       # task timeout
        soft_fail=True                       # skipped after timeout instead of failing
    )