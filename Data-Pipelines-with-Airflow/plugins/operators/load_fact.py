from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    insert_query = """
        INSERT INTO {table} 
        {select_query}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table = '',
                 select_query = '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_query = select_query

    def execute(self, context):
        self.log.info(f"Loading data to {self.table}...")
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        redshift.run(LoadFactOperator.insert_query.format(
            table = self.table,
            select_query = self.select_query
        ))