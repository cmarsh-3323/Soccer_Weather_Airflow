from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 tables = None,
                 redshift_conn_id = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info("Beginning the DataQualityOperator...")
        # Connecting to redshift
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        # Looping through tables
        for table in self.tables:
            if table != "public.Dim_Weather_Country": #dim_weather duplicates country and dates and has no id
                # Checking for duplicate records
                duplicate_check_query = f"""
                select id, count(id)
                from {table}
                group by id
                having count(id) > 1
                """
                duplicate_check_result = redshift_hook.get_records(duplicate_check_query)

                if duplicate_check_result and duplicate_check_result[0][0] > 0:
                    self.log.info(f"Data quality check has failed for table {table}!!! It has duplicate records")
                    raise ValueError(f"Data quality check has failed for table {table}!!! It has duplicate records")
            
            # Check if table is empty or has no records
            table_data = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(table_data) < 1 or table_data[0][0] == 0 or len(table_data[0]) < 1:
                self.log.info(f"Data quality check has failed for table {table}!!! It returned no results")
                raise ValueError(f"Data quality check has failed for table {table}!!! It returned no results")
                
        self.log.info("All data quality checks have passed!")