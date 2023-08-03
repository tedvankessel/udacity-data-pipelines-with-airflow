# add AWS Redshift connection
airflow connections add redshift --conn-uri 'redshift://awsuser:Fat2q23cat@default-workgroup.595917921970.us-east-1.redshift-serverless.amazonaws.com:5439/dev'
# add S3 bucket varaible
airflow variables set s3_bucket tgvkbucket
# add aws credentials
airflow connections add aws_credentials --conn-type 'aws' --conn-login 'AKIAYVP3Z32ZLAWBPSRW' --conn-password 'Ma2LWc/ifge5GElvZ6aRcuq8HJgeiQ4LXgD0GpLL'