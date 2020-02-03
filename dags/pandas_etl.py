from airflow.models import DAG
from datetime import datetime, timedelta
import logging as log

from ..operators.pandas_etl_over_postgres_operator import PandasETLOverPostgresOperator

default_arguments = {
    'owner': 'airflow',
    'start_date': datetime(2019, 5, 15, 0, 0, 0),
    'retries': 3,
    'retry_delay': timedelta(minutes=10)
}

dag = DAG(dag_id='pandas_etl',
          max_active_runs=5,
          schedule_interval='0 * * * *',
          default_args=default_arguments,
          catchup=False)


def etl(df):
    result = df.groupby(['dag_id', 'state']).size()
    for elements in result.items():
        log.info(elements[0][0] + ' - ' + elements[0][1] + ' = ' + str(elements[1]))


SQL_QUERY = 'select dag_id, state from dag_run;'
CONNECTION_ID = 'pandas_etl'

pandas_etl = PandasETLOverPostgresOperator(task_id='pandas_etl_over_postgres',
                                           dag=dag,
                                           etl_function=etl,
                                           sql_query=SQL_QUERY,
                                           connection_id=CONNECTION_ID)

pandas_etl
