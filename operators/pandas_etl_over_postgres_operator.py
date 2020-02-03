import logging as log

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class PandasETLOverPostgresOperator(BaseOperator):

    @apply_defaults
    def __init__(self, connection_id, sql_query, etl_function, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connection_id = connection_id
        self.sql_query = sql_query
        self.etl_function = etl_function

    def execute(self, context):
        log.info('Run Pandas over postgres')
        postgres_instance = PostgresHook(postgres_conn_id=self.connection_id)
        df = postgres_instance.get_pandas_df(self.sql_query)
        self.etl_function(df)