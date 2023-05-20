import pandas as pd
from typing import List
import contextlib
import vertica_python

class loader:
    def __init__(self) -> None:
        self._db = vertica_python.connect(
            host='vertica.tgcloudenv.ru',
            port=5433,
            user='farruhrusyandexru',
            password="JLlMNB9gxWc5A0h"
        )

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
            cur.execute(
                """
                    INSERT INTO FARRUHRUSYANDEXRU__DWH.l_user_group_activity
                        (hk_l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)
                    select distinct hash(gl.group_id,gl.user_id),
                        hu.hk_user_id ,
                        hg.hk_group_id ,
                        datetime,
                        's3' as load_src
                    from FARRUHRUSYANDEXRU__STAGING.group_log as gl
                    left join FARRUHRUSYANDEXRU__DWH.h_users hu on gl.user_id = hu.user_id 
                    left join FARRUHRUSYANDEXRU__DWH.h_groups hg on gl.group_id = hg.group_id
                    LIMIT 100; -- test
                """
            )
            vertica_conn.commit()
            vertica_conn.close()
    def load_satellite(self):
        vertica_conn = self._db
        with vertica_conn.cursor() as cur:
            print('insert started')
            cur.execute(
                """
                    INSERT INTO FARRUHRUSYANDEXRU__DWH.s_auth_history
                        (hk_l_user_group_activity, user_id_from,event,event_dt,load_dt,load_src)
                    select 
                        luga.hk_l_user_group_activity,
                        gl.user_id_from,
                        gl.event,
                        gl.datetime as event_dt,
                        now() as load_dt,
                        's3' as load_src
                    from FARRUHRUSYANDEXRU__STAGING.group_log as gl
                    left join FARRUHRUSYANDEXRU__DWH.h_groups as hg on gl.group_id = hg.group_id
                    left join FARRUHRUSYANDEXRU__DWH.h_users as hu on gl.user_id = hu.user_id
                    left join FARRUHRUSYANDEXRU__DWH.l_user_group_activity as luga on 
                        hg.hk_group_id = luga.hk_group_id 
                        and hu.hk_user_id = luga.hk_user_id
                    LIMIT 100; -- test
                """
            )
            vertica_conn.commit()
            vertica_conn.close()
            print('insert ended')
