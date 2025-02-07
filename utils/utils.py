
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook


def extract_data_to_xlsx(sql_query, postgres_conn_id, export_path):
    conn = PostgresHook(postgres_conn_id).get_conn()
    df = pd.read_sql('SELECT * FROM weather_data_description', conn)
    conn.close()

    with pd.ExcelWriter(f'{export_path}', engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Weather Data', index=False)
