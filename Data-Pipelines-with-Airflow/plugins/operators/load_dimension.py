from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    truncate_query = """
        TRUNCATE TABLE {table}
    """
    insert_query = """
        INSERT INTO {table} 
        {select_query}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table = '',
                 select_query = '',
                 insert_mode = 'truncate',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_query = select_query
        self.insert_mode = insert_mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        if self.insert_mode not in ('truncate', 'append'):
            raise ValueError('Unknown insert mode!')

        if self.insert_mode == 'truncate':
            self.log.info(f"Truncating {self.table}...")
            formatted_sql = LoadDimensionOperator.truncate_query.format( table = self.table )
            redshift.run( formatted_sql )
            
        if self.insert_mode in ( 'truncate', 'append' ):
            self.log.info(f"Loading data to {self.table}...")
            formatted_sql = LoadDimensionOperator.insert_query.format(
                table = self.table, 
                select_query = self.select_query)
            redshift.run( formatted_sql )