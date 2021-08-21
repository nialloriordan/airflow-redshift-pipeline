from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """Copy JSON files from S3 to staging table in Redshift

    Atributes:
        redshift_conn_id (str): Airflow connection name for Redshift
        aws_credentials_id (str): Airflow connection name for AWS credentials
            to access S3
        table (str): name of staging table in Redshift
        s3_bucket (str): name of S3 bucket to copy data from
        s3_key (str): subfolders within S3 bucket
        s3_region (str): region in which S3 bucket is located
        json_schema (str, optional): JSON schema to use when copying data to Redshift.
            Defaults to 'auto'.
        truncate (bool, optional): Remove existing rows from table via
            `Truncate`. Defaults to False.
    """

    ui_color = "#358140"
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {TABLE}
        FROM '{S3_PATH}'
        ACCESS_KEY_ID '{AWS_ACCESS_KEY}'
        SECRET_ACCESS_KEY '{AWS_SECRET_ACCESS_KEY}'
        FORMAT AS JSON '{JSON_SCHEMA}'
        REGION '{S3_REGION}';
    """

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id: str,
        aws_credentials_id: str,
        table: str,
        s3_bucket: str,
        s3_key: str,
        s3_region: str,
        json_schema="auto",
        truncate=False,
        *args,
        **kwargs
    ):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_region = s3_region
        self.json_schema = json_schema
        self.truncate = truncate

    def execute(self, context):
        aws_conn = BaseHook.get_connection(self.aws_credentials_id)
        aws_access_key = aws_conn.login
        aws_secret_access_key = aws_conn.password
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        if self.truncate:
            redshift_hook.run("TRUNCATE TABLE {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            TABLE=self.table,
            S3_PATH=s3_path,
            AWS_ACCESS_KEY=aws_access_key,
            AWS_SECRET_ACCESS_KEY=aws_secret_access_key,
            JSON_SCHEMA=self.json_schema,
            S3_REGION=self.s3_region,
        )
        redshift_hook.run(formatted_sql)
        self.log.info("Finished copying data from S3 to Redshift")
