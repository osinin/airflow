dags = ['apelsin__firebase_bigquery__ch_events__etl', 'kapitalbank__firebase_bigquery__ch_events__etl',
        'nasiya__firebase_bigquery__ch_events__etl', 'uzumbank__ch__operation_w_cashback__dag',
        'marts__s3__market__extraction', 'uzumbank_s3_tezkor_extraction_etl', 'kapital_s3_oracle_extraction_etl',
        'nasiya_s3_ch', 'raw__ecosystem_identifiers_days__etl']


def get_dag_runs() -> None:
    ch_client.execute_query("raw", f"TRUNCATE TABLE raw.airflow_dag_run_info")
    log.info("raw.airflow_dag_run_info truncated")
    for dag_id in dags:
        log.info(f"getting {dag_id} runs ...")
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
        df = df[(df.state.isin(["success", "failed"])) & (df.conf != "None")]
        ch_client.insert_pandas_df_nullable("raw", "airflow_dag_run_info", df)


def get_new_runs(script_name='raw_airflow_dag_run_info', **kwargs) -> list:
    ts = kwargs["ts"]
    ts = ts.replace("T", " ")[:19]
    log.info(f"ts: {ts}, {type(ts)}")
    sql = open(os.path.join(conf.get("core", "dags_folder"), "uzum", "raw_dags",
                                        "ecosystem_identifiers_days", "queries", f"{script_name}.sql")).read()
    sql = re.sub("@ts", ts, sql)
    log.info(f"SQL:\n{sql}")
    partitions = ch_client.execute_query("raw", sql)
    log.info(f"partitions: {partitions}, {type(partitions)}")
    return partitions


def listener(**kwargs) -> None:
    partitions = kwargs["partitions"]
    partitions = ast.literal_eval(partitions)
    log.info(f'script_name: {script_name}')
    if len(partitions) == 0:
        raise AirflowSkipException(f'There are no updates in the parents DAGs')