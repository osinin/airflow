import ast
import os
import pandas as pd
import logging

from airflow.configuration import conf
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import DagRun, Variable

from common.ch import ClickhouseClient

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

dags = ['parent_dag_1', 'parent_dag_2', 'parent_dag_3', 'child_dag']


def get_dag_runs() -> None:
    table_name = 'xxx'
    ch_client.execute_query("raw", f"TRUNCATE TABLE {table_name}")
    log.info(f"{table_name} truncated")
    for dag_id in dags:
        dag_runs = DagRun.find(dag_id=dag_id)
        columns = ['dag_id', 'start_date_utc', 'end_date_utc', 'execution_date_utc', 'state', 'conf']
        df = pd.DataFrame([], columns=columns)
        for dag_run in dag_runs:
            conf = dag_run.conf.get('partitions')
            data = [dag_run.dag_id, dag_run.start_date, dag_run.end_date,
                    dag_run.execution_date, dag_run.state, conf]
            new_row = pd.DataFrame([data], columns=columns)
            df.start_date_utc.fillna('1970-01-01 00:00:00', inplace=True)
            df.end_date_utc.fillna('1970-01-01 00:00:00', inplace=True)
            df.execution_date_utc.fillna('1970-01-01 00:00:00', inplace=True)
            df = pd.concat([new_row, df])
        df = df[(df.state == "success") & (df.conf != "None")]
        # insert data into CH table
        ch_client.insert_pandas_df_nullable("db_aaa", table_name, df)


def get_new_runs() -> list:
    script_name = 'airflow_dag_runs'
    sql = open(os.path.join(conf.get("core", "dags_folder"), "sql", "scripts", f"{script_name}.sql")).read()
    log.info(f"SQL: {sql}")
    # find new DAG runs in CH table
    partitions = ch_client.execute_query("db_aaa", sql)
    return partitions


def insert_df_into_sandbox(**kwargs) -> None:
    partitions = kwargs["partitions"]
    partitions = ast.literal_eval(partitions)
    if len(partitions) == 0:
        raise AirflowSkipException(f'there are no updates in the parents DAGs ')
    for part in partitions:
        # use the parts to calculate partitions in the child DAG
        pass

"""
get partitions for operator form kwargs

    insert_df_into_sandbox_task = PythonOperator(
        task_id=f"insert_{script_name}",
        python_callable=insert_df_into_sandbox,
        dag=dag,
        op_kwargs={"partitions": "{{ task_instance.xcom_pull(task_ids='get_new_runs', key='return_value') }}"}
        )
"""