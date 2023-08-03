"""
load_fact_tvk.py
used by: tvkDAGv2.py program
used for: Udacity Automate Data Piplines Project
2023-08-02
for details, sources etc. see README.md
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from final_project_operators.sqlqueries_tvk import SqlQueries

class LoadFactOperator(BaseOperator):
    """
    this class loads the fact table used in the tvkDAGv2.py program
    """
    ui_color = '#F98866'
    # initialize default parameters
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 sql_create = "",
                 sql = "",        
                 append_only = False,
                 *args, **kwargs):
                    
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        # load passed parameters
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_create = sql_create
        self.sql = sql
        self.append_only = append_only
    
    # execute with passed parameters
    def execute(self, context):
        # redshift hook to get a connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # create the fact table if it does not exist
        self.log.info("Create fact table if it does not exist")
        formatted_sql = getattr(SqlQueries,self.sql_create).format(self.table)

        # allow for append only operation - not used yet
        # copy data into fact table from staging table
        if not self.append_only:
            redshift.run(formatted_sql)
            self.log.info("Delete {} fact table".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))         
        self.log.info("Insert data from staging tables into {} fact table".format(self.table))
        formatted_sql = getattr(SqlQueries,self.sql).format(self.table)
        redshift.run(formatted_sql)

        # get a row count and log it
        result = redshift.get_records("""
        SELECT COUNT(*) 
        FROM {};
        """.format(self.table))
        row_count = result[0][0]  # Extract the row count from the result
        self.log.info("Number of rows in {}: {}".format(self.table, row_count))

        # get column headers and first 5 rows of data and log it
        result = redshift.get_records("""SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{}';""".format(self.table))
        self.log.info(result)
        records = redshift.get_records("SELECT * FROM {} LIMIT 5".format(self.table)) 
        self.log.info(records)