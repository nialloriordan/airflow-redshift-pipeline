from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """Load Redshift fact table operator

    Attributes:
        redshift_conn_id (str): Airflow connection name for Redshift.
        sql_statement (str): SQL query for select statement
        target_table (str): name of target table in Redshift
    """

    ui_color = "#F98866"

    facts_sql_template = """
    INSERT INTO {TABLE}
    {SELECT_STATEMENT};
    """

    @apply_defaults
    def __init__(
        self, redshift_conn_id: str, sql_statement: str, target_table: str, *args, **kwargs
    ):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement
        self.target_table = target_table

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        formatted_sql = LoadFactOperator.facts_sql_template.format(
            TABLE=self.target_table, SELECT_STATEMENT=self.sql_statement
        )
        self.log.info("Loading fact table into Redshift...")
        redshift_hook.run(formatted_sql)
        self.log.info("Finished loading fact table into Redshift")
