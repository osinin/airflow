-- We need to get all execution dates (logical or dates in conf) of parent's DAGs
-- which have ended later than the last successful child DAG

with end_dates as (
select toYYYYMM(execution_date_utc) as mnth
from raw.airflow_dag_run_info
where dag_id not in ('parent_dag_1', 'parent_dag_2')
and end_date_utc >= (select max(end_date_utc)
					 from raw.airflow_dag_run_info
					 where dag_id = 'child_dag')
),

conf_dates as (
select
position(arrayJoin(splitByChar(',', replaceAll(replaceAll(conf, '[', ''), ']', ''))), '2') as pos,
arrayJoin(splitByChar(',', replaceAll(replaceAll(conf, '[', ''), ']', ''))) as ddate,
substr(ddate, pos, 6) as mnth
from raw.airflow_dag_run_info
where dag_id = 'parent_dag_3'
and end_date_utc >= (select max(end_date_utc)
					 from raw.airflow_dag_run_info
					 where dag_id = 'child_dag')
)
select * from end_dates
union distinct
select toUInt32(mnth) from conf_dates