"""
stage_redshift_tvk.py
used by: tvkDAGv2.py program
used for: Udacity Automate Data Piplines Project
2023-08-02
for details, sources etc. see README.md
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from final_project_operators.sqlqueries_tvk import SqlQueries

class StageToRedshiftOperator(BaseOperator):
    """
    this class implements the transfer of data from the Amazon S3 to Amazon Redshift tables. 
    """

    ui_color = '#358140'

    # scripts to copy data with parameters for connections, login codes etc.

    copy_sql_date = """
        COPY {} FROM '{}/{}/{}/'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        {} 'auto ignorecase'
        ACCEPTINVCHARS
        MAXERROR 1000;
        """

    copy_sql = """
        COPY {} FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        {} 'auto ignorecase'
        ACCEPTINVCHARS
        MAXERROR 1001;
    """
    # class default parameters
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_conn_id="",
                 table = "",
                 s3_path = "",
                 region= "us-east-1",
                 data_format = "",
                 sql = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.s3_path = s3_path
        self.region = region
        self.data_format = data_format
        self.sql = sql
        self.execution_date = kwargs.get('execution_date')
    # class execution code
    def execute(self, context):
        # hook to get redshift credentials
        aws = AwsHook(self.aws_conn_id)
        credentials = aws.get_credentials()

        # hook to get a redshift connection
        self.log.info(f"tvk-temp-debug aws creds {credentials}")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # delete existing table 
        self.log.info("Deleting existing table")
        try:
            redshift.run("DROP TABLE {}".format(self.table))
        except Exception as e:
            self.log.info(e)

        # create table if it does not exist
        self.log.info("Create destination Redshift table if it does not exist")
        formatted_sql = getattr(SqlQueries,self.sql).format(self.table)
        redshift.run(formatted_sql)

        # delete any residual data
        self.log.info("Deleting data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        # log column headers
        self.log.info("check table columns")

        result = redshift.get_records("""SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{}';""".format(self.table))
        self.log.info(result)

        # copy data into table 
        self.log.info("Copying data from S3 to Redshift")
        # Backfill a specific date
        if self.execution_date:
            self.log.info("Path 1")
            formatted_sql = StageToRedshiftOperator.copy_sql_date.format(
                self.table, 
                self.s3_path, 
                self.execution_date.strftime("%Y"),
                self.execution_date.strftime("%d"),
                credentials.access_key,
                credentials.secret_key, 
                self.region,
                self.data_format,
                self.execution_date
            )
        else:
            self.log.info("Path 2")
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table, 
                self.s3_path, 
                credentials.access_key,
                credentials.secret_key, 
                self.region,
                self.data_format,
                self.execution_date
            )
        self.log.info("self.execution_date")
        self.log.info("Execution Date: %s", self.execution_date)
        # log query info to confirm
        self.log.info("query info:")
        self.log.info(formatted_sql)
        # try to copy data
        try:
            redshift.run(formatted_sql)
        except Exception as e:
            self.log.info(e)
            result = redshift.run("""
                SELECT *
                FROM sys_load_error_detail
                ORDER BY start_time DESC
                LIMIT 10;
                """)
            rows = result.fetchall()
            for row in rows:
                self.log.info(row)
        
        # check and log table dimensions column headers and number of rows
        self.log.info("check final table dimensions")
        result = redshift.get_records("""SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{}';""".format(self.table))
        self.log.info(result)

        result = redshift.get_records("""
        SELECT COUNT(*) 
        FROM {};
        """.format(self.table))
        row_count = result[0][0]  # Extract the row count from the result

        self.log.info("Number of rows in {}: {}".format(self.table, row_count))

        # log column headers (again) and first five rows of data
        result = redshift.get_records("""SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{}';""".format(self.table))
        self.log.info(result)
        records = redshift.get_records("SELECT * FROM {} LIMIT 5".format(self.table)) 
        self.log.info(records[0])
        self.log.info(records[1])
        self.log.info(records[2])
        self.log.info(records[3])