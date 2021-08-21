from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    """Load Redshift dimension table operator

    Attributes:
        redshift_conn_id (str): Airflow connection name for Redshift.
        sql_statement (str): SQL query for select statement
        target_table (str): name of target table in Redshift
        truncate (bool, optional): Remove existing rows from table via
            `Truncate`. Defaults to False.
    """

    ui_color = "#80BD9E"

    dim_sql_template = """
    {TRUNCATE_STATEMENT}
    INSERT INTO {TABLE}
    {SELECT_STATEMENT};
    """

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id: str,
        sql_statement: str,
        target_table: str,
        truncate=False,
        *args,
        **kwargs,
    ):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement
        self.target_table = target_table
        self.truncate = truncate

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        truncate_statement = f"TRUNCATE TABLE {self.target_table};" if self.truncate else ""

        formatted_sql = LoadDimensionOperator.dim_sql_template.format(
            TRUNCATE_STATEMENT=truncate_statement,
            TABLE=self.target_table,
            SELECT_STATEMENT=self.sql_statement,
        )
        self.log.info("Loading dimension table into Redshift...")
        redshift_hook.run(formatted_sql)
        self.log.info("Finished dimension table into Redshift")
