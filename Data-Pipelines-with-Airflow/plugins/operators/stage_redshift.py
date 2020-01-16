from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_query = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS 'us-west-2'
        {}
        COMPUPDATE OFF
        TIMEFORMAT AS 'epochmillisecs'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table = '',
                 s3_bucket = '',
                 s3_key = '',
                 aws_credentials_id = '',
                 file_format = '',
                 json_path = 'auto',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.file_format = file_format
        self.json_path = json_path

    def execute(self, context):
        if self.file_format not in ('csv', 'json'):
            raise ValueError(f"file format {self.file_format} is not csv or json")
        if self.file_format == 'json':
            file_format = "FORMAT AS JSON '{}'".format(self.json_path)
        else:
            file_format = "FORMAT AS CSV"

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_query.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            file_format,
        )
        redshift.run(formatted_sql)