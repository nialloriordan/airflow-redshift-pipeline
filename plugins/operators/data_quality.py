from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
from typing import Dict, Callable


class DataQualityOperator(BaseOperator):
    """Redshift data quality operator

    Attributes:
        redshift_conn_id (str): Airflow connection name for Redshift.
        sql_queries_results (Dict[str, Callable[int, bool]]): Dictionary of sql queries and
            expected number of records formatted as a lambda function.

    Raises:
        ValueError: Error if query returns no results
        ValueError: Error if number of records from query
            doesn't match expected number of records
    """

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id: str,
        sql_queries_results: Dict[str, Callable[[int], bool]],
        *args,
        **kwargs,
    ):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_queries_results = sql_queries_results

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for query, result_function in self.sql_queries_results.items():
            logging.info(f"Running query check: {query}")
            records = redshift_hook.get_records(query)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {query} returned no results")
            num_records = records[0][0]
            if result_function(num_records) is False:
                raise ValueError(
                    f"Data quality check failed. Query returned {num_records} records."
                )
            logging.info(
                f"Data quality check for query {query} passed with {num_records} records as expected"
            )
