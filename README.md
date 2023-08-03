# udacity-data-pipelines-with-airflow
## <p>Automate Data Pipelines Project 4
## Introduction 
(from the project introduction)
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.
## Datasets
## Scema
## DAG setup
The suggested individual DAG tasks are shown below (from project docs):

![Project_DAG_in_the_Airflow_UI](./image_data/Project_DAG_in_the_Airflow_UI.png)

The suggested DAG flow configuration is shown below (from project docs:

![example_DAG](./image_data/Example_DAG.png)

## Rubric elements

The following sections go through each rubric item and provide evidence of it's
being done.

### Rubric item: Prerequisites
The following prerequisites were satisfied from the earlier lessons and were used
project:

	Create an IAM User in AWS.
	Configure Redshift Serverless in AWS.
	Connect Airflow and AWS
	Creat AWS Redshift credentials in Airflow
	Create AWS S3 connection in Airflow
	Connect Airflow to AWS Redshift Serverless
 
I created the auxillary file **airflow_setup_all.sh** to set up the airflow
connections:

	# add AWS Redshift connection
	airflow connections add redshift 
 		note: line truncated to avoid disclosure of redshift credentials
	# add S3 bucket varaible
	airflow variables set s3_bucket tgvkbucket
	# add aws credentials
	airflow connections add aws_credentials 
 		note: line truncated to avoid disclosure of aws credentials
### Rubric item: General

	The dag and plugins do not give an error when imported to Airflow
	All tasks have correct dependencies
 
 This is shown in the following screenshot of the loaded tvkDAGv2.py program
 with the associated dependency graph:
 ![task dependencies](./image_data/tvkDAGv2_task_dependencies.png)

### Rubric item: Dag configuration

	* Default_args object is used in the DAG
	* Defaults_args are bind to the DAG
	* The DAG has a correct schedule
 
 The following is the code used to set the defaul_args. Also see tvkDAGv2.py
 
	 default_args = {
	    'owner': 'T_van_Kessel',
	    'start_date': datetime(2019, 1, 12),
	    'depends_on_past': False,
	    'retries': 3,
	    'retry_delay': timedelta(minutes=1),
	    'catchup_by_default': False,
	    'email_on_retry': False
	}
 The details of the default conviguration and schedule are shown in the following screenshot:
 ![task details](./image_data/tvkDAGv2_details.png)

### Rubric item: Staging the data

	* Task to stage JSON data is included in the DAG and uses the RedshiftStage operator
	* Task uses params
	* Logging used
	* The database connection is created by using a hook and a connection

 Two tasks were created for this purpose, **stage_events_to_redshift** and
 **stage_songs_to_redshift**. The full details of the implementation can be found in
 the tvkDAGv2.py, stage_redshift_tvk and the sqlqueries_tvk.py files. These embody all
 aspects of the rubric. The code snippets below show the tasks from the tvkDAGv2.py file. 

	 stage_events_to_redshift = StageToRedshiftOperator(
	    task_id='Stage_events',
	    dag=dag,
	    table = "staging_events",
	    s3_path = "s3://tgvkbucket/log-data",
	    redshift_conn_id="redshift",
	    aws_conn_id="aws_credentials",
	    region="us-east-1",
	    data_format="JSON",
	    sql = "staging_events_table_create"
	)
	stage_songs_to_redshift = StageToRedshiftOperator(
	    task_id='Stage_songs',
	    dag=dag,
	    table = "staging_songs",
	    s3_path = "s3://tgvkbucket/song-data",
	    redshift_conn_id="redshift",
	    aws_conn_id="aws_credentials",
	    region="us-east-1",
	    data_format="JSON",
	    sql = "staging_songs_table_create"
	)

### Rubric Item: Loading dimensions and facts

	* Set of tasks using the dimension load operator is in the DAG
	* A task using the fact load operator is in the DAG
	* Both operators use params
	* The dimension task contains a param to allow switch between append and insert-delete functionality

### Rubric Item: Data Quality Checks

	* A task using the data quality operator is in the DAG and at least one data quality check is done
	* The operator raises an error if the check fails pass
	* The operator is parametrized


### Installing
## References
## License
This project is licensed under the Apache 2.0  License - see the LICENSE.md file for details
## Built With
Starter code was provided by Udacity as follows:

	/home/workspace/airflow/plugins/final_project_operators/data_quality.py
	/home/workspace/airflow/plugins/final_project_operators/load_fact.py
	/home/workspace/airflow/plugins/custom_operators/load_dimensions.py
	/home/workspace/airflow/plugins/final_project_operators/stage_redshift.py
	/home/workspace/airflow/dags/udacity/common/final_project_sql_statements.py
	/home/workspace/airflow/dags/cd0031-automate-data-pipelines/project/starter/final_project.py
## Authors
* **Theodore van Kessel** 
## Acknowledgments and sources
	Udacity project documents 
	README-Template - https://gist.github.com/PurpleBooth/109311bb0361f32d87a2
	ChatGPT
	Udacity GPT
	Google
	Github references: 

