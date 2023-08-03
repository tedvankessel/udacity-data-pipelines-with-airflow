"""
data_quality.py
used by: tvkDAGv2.py program
used for: Udacity Automate Data Piplines Project
2023-08-02
for details, sources etc. see README.md
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    this class performs quality tests on a list of tables passed as parameters from the tvkDAGv2.py program.
    In particular it checks each table and verifies that the table contains data
    """
    ui_color = '#89DA59'

    # initialize default parameters
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        
    # execute with passed parameters
    def execute(self, context):
        # redshift hook to get a connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # cycle through each table passed as a list in the parameter "tables" 
        for table in self.tables:
            try:
                # try to count the rows in the table and raise error if no data found
                records = redshift.get_records("SELECT COUNT(*) FROM {}".format(table))        
                if len(records) < 1 or len(records[0]) < 1:
                    self.log.error("{} returned no results".format(table))
                    raise ValueError("Data quality check failed. {} returned no results".format(table))
                num_records = records[0][0]
                if num_records == 0:
                    self.log.error("No records present in destination table {}".format(table))
                    raise ValueError("No records present in destination {}".format(table))
                self.log.info("Data quality on table {} check passed with {} records".format(table, num_records))
            except:
                # log exception
                self.log.info("Table {} failed".format(table))