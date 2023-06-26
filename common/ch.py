from clickhouse_driver import connect
from clickhouse_driver import Client
import logging
import pandas as pd
import re
from simple_ddl_parser import DDLParser
import numpy as np


logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s')
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


class ClickhouseClient:
    """"""

    def __init__(self, host, port, user, password, connect_timeout):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.connect_timeout = connect_timeout

    def get_db_connect(self, db):
        return connect(host=self.host,
                       port=self.port,
                       database=db,
                       user=self.user,
                       password=self.password,
                       connect_timeout=self.connect_timeout)

    def get_db_client(self, db, use_numpy=True, send_timeout=600, receive_timeout=600):
        return Client(host=self.host,
                      port=self.port,
                      database=db,
                      user=self.user,
                      password=self.password,
                      settings={
                                'use_numpy': use_numpy,
                                'allow_experimental_lightweight_delete': True,
                                'receive_timeout': receive_timeout,
                                'send_timeout': send_timeout
                                }
                      )

    def execute(self, db, query, params=None):
        client = self.get_db_client(db)
        return client.execute(query, params)

    def execute_query(self, db, query):
        conn = self.get_db_connect(db)
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def get_ddl(self, db, table_name):
        query = f"show create table {table_name}"
        table_ddl = self.execute_query(db, query)
        return table_ddl[0][0].replace('`', '')

    def get_columns(self, db, table_name):
        query = f"describe {db}.{table_name}"
        query_result = self.execute_query(db, query)
        return list(map(lambda c: f"{c[0]}", query_result))

    def get_columns_with_types(self, db, table_name):
        query = f"describe {db}.{table_name}"
        query_result = self.execute_query(db, query)
        list_columns = list(map(lambda c: f"{c[0]} {c[1]}", query_result))
        return ",".join(list_columns)

    def convert_to_datetime(self, db, table_name, df):
        cols = self.get_columns_with_types(db, table_name).split(",")
        for col in range(len(cols)):
            column_name = cols[col].split()[0]
            column_type = cols[col].split()[1]
            if 'Date' in column_type:
                df[column_name] = pd.to_datetime(df[column_name], errors='ignore')
                log.info(f"{column_name} column type changed to Datetime")
        return df
    def convert_to_int(self, db, table_name, df):
        cols = self.get_columns_with_types(db, table_name).split(",")
        for col in range(len(cols)):
            column_name = cols[col].split()[0]
            column_type = cols[col].split()[1]
            if 'Int' in column_type and 'Nullable' in column_type:
                df[column_name] = df[column_name].astype(str).apply(lambda x: x.replace('.0', ''))\
                    .replace('nan', None, regex=True)
                log.info(f"{column_name} column type changed to Int")
        return df

    def insert_pandas_df_nullable(self, db, table, df, insert_block_size=10000, hard_convert_to_int=False):
        client = self.get_db_client(db, use_numpy=True)
        list_columns = [x for x in self.get_columns(db, table) if x in list(df.columns)]
        log.info(df.info())
        df = self.convert_to_datetime(db, table, df)
        if hard_convert_to_int:
            df = self.convert_to_int(db, table, df)
        result = client.insert_dataframe(f"INSERT INTO {table} ({','.join(list_columns)}) VALUES",
                                         df.replace(np.nan, None, regex=True)[list_columns])
        return result

    def insert_pandas_df(self, db, table, df, insert_block_size=10000):
        client = self.get_db_client(db)
        list_columns = [x for x in self.get_columns(db, table) if x in list(df.columns)]
        # list_columns = self.get_columns(db, table)
        df = self.convert_to_datetime(db, table, df)
        return client.insert_dataframe(f"INSERT INTO {table} ({','.join(list_columns)}) VALUES", df.astype(str)[list_columns])

    def create_sandbox_from_source(self, source_db, source_table, change_engine=None):
        source_ddl = self.get_ddl(source_db, source_table)
        sandbox_table_ddl = source_ddl.replace(f"CREATE TABLE {source_db}", "CREATE TABLE IF NOT EXISTS sandbox")
        if change_engine:
            sandbox_table_ddl = re.sub("ENGINE.+\n", "ENGINE = MergeTree \n", sandbox_table_ddl)
        self.execute_query("sandbox", sandbox_table_ddl)

    def get_list_partitions(self, db, table):
        query = f"""
            SELECT distinct partition FROM `system`.parts
            where table = '{table}'
            and database = '{db}'
            """
        return [x[0] for x in self.execute_query(db, query)]

    def replace_partition(self, source_db, target_db, table_name, partition):

        log.info(f'Partition: {partition}')
        query = f"""
                 ALTER TABLE {target_db}.{table_name} REPLACE PARTITION '{partition}' FROM {source_db}.{table_name} 
                """
        #if table has no partiotions
        query = query.replace("'tuple()'", "tuple()")
        log.info(f"query: {query}")
        return self.execute_query(target_db, query)

    def fill_na_by_ddl(self, db, table, df=None, day=None, nullable_columns=[]):

        ddl = self.get_ddl(db, table)
        ddl = re.sub('\sCODEC.+,\n', ',\n', ddl)
        ddl = ddl.replace('Nullable(DateTime64', 'DateTime64').replace(')),', '),')
        pattern = r'(.+?)ENGINE'
        result = re.search(pattern, ddl, re.S)
        if result:
            parse_results = DDLParser(result.group(1)).run()
            columns = parse_results[0]['columns']

            columns = list(map(lambda c: c, filter(lambda c: c['name'] not in nullable_columns, columns)))

            # date type
            date_columns = list(map(lambda c: c['name'], filter(lambda c: 'Date' in c['type'], columns)))
            date_columns = list(set(date_columns) & set(df.columns))
            if len(date_columns) > 0:
                df[date_columns] = df[date_columns].fillna('1970-01-01 05:00:00')
                # fix dates if future
                for col in date_columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    if day:
                        df[col] = df[col].apply(
                            lambda x: '1970-01-01 05:00:00' if x.year > int(day[:4]) else x)

            # float type
            float_columns = list(map(lambda c: c['name'], filter(lambda c: re.match(r"Float", c['type']), columns)))
            float_columns = list(set(float_columns) & set(df.columns))
            if len(float_columns) > 0:
                df[float_columns] = df[float_columns].fillna(0)

            # integer type
            int_columns = list(map(lambda c: c['name'], filter(lambda c: re.match(r"UInt|Int", c['type']), columns)))
            int_columns = list(set(int_columns) & set(df.columns))
            if len(int_columns) > 0:
                df[int_columns] = df[int_columns].fillna(0).astype('int')

            # string type
            string_columns = list(map(lambda c: c['name'], filter(lambda c: 'String' in c['type'], columns)))
            string_columns = list(set(string_columns) & set(df.columns))
            if len(string_columns) > 0:
                df[string_columns] = df[string_columns].fillna('')

        else:
            logging.info(f"impossible to parse {db}.{table}")

        return df
