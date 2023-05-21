import pandas as pd
import os
from typing import List
import contextlib
import vertica_python
from logging import Logger

class loader:
    def __init__(self, log: Logger) -> None:
        self._db = vertica_python.connect(
            host='vertica.tgcloudenv.ru',
            port=5433,
            user='farruhrusyandexru',
            password="JLlMNB9gxWc5A0h",
            autocommit=True
        )
        self.log = log

    def load_csv(self, dataset_path: str,schema: str, table: str, columns: List[str]):
        df = pd.read_csv(dataset_path)
        df['user_id_from'] = pd.array(df['user_id_from'], dtype="Int64")
        num_rows = len(df)
        vertica_conn = self._db
        columns = ', '.join(columns)
        copy_expr = f"""
        COPY {schema}.{table} ({columns}) FROM STDIN DELIMITER ',' ENCLOSED BY ''''
        """
        chunk_size = num_rows // 100
        with contextlib.closing(vertica_conn.cursor()) as cur:
            start = 0
            while start <= num_rows:
                end = min(start + chunk_size, num_rows)
                print(f"loading rows {start}-{end}")
                df.loc[start: end].to_csv(f'/data/{table}.csv', index=False)
                with open(f'/data/{table}.csv', 'rb') as chunk:
                    cur.copy(copy_expr, chunk, buffer_size=65536)
                vertica_conn.commit()
                print("loaded")
                start += chunk_size + 1
    
        vertica_conn.close()
    
    def load_link(self):
        vertica_conn = self._db
        with vertica_conn.cursor() as cur:
            self.log.info('insert in l_user_group_activity started')
            query=open("/lessons/dags/project/sql/insert_link.sql", "r").read()
            cur.execute(query)
            self.log.info('insert in l_user_group_activity finished')
    
    def load_satellite(self):
        vertica_conn = self._db
        with vertica_conn.cursor() as cur:
            self.log.info('insert in s_auth_history started')
            query=open("/lessons/dags/project/sql/insert_satellite.sql", "r").read()
            cur.execute(query)
            self.log.info('insert in s_auth_history finished')
