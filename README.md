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
The suggested DAG flow configuration is shown below:

![Project_DAG_in_the_Airflow_UI](./image_data/Project_DAG_in_the_Airflow_UI.png)

![example_DAG](./image_data/Example_DAG.png)

### Prerequisites
	Create an IAM User in AWS.
	Configure Redshift Serverless in AWS.
	Connect Airflow and AWS
	Creat AWS Redshift credentials in Airflow
	Create AWS S3 connection in Airflow
	Connect Airflow to AWS Redshift Serverless

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
	README-Template - https://gist.github.com/PurpleBooth/109311bb0361f32d87a2
	ChatGPT
	Udacity GPT
	Google
	Github references: 

