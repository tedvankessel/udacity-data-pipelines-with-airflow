# add AWS Redshift connection
airflow connections add redshift --conn-uri 'redshift://awsuser:xxxxxxx@default-workgroup.595917921970.us-east-1.redshift-serverless.amazonaws.com:5439/dev'
# add S3 bucket varaible
airflow variables set s3_bucket xxxxxxxxx
# add aws credentials
airflow connections add aws_credentials --conn-type 'aws' --conn-login 'xxxxxxxxx' --conn-password 'xxxxxxxxxxx'
