Install AWS CDK (https://docs.aws.amazon.com/cdk/latest/guide/getting_started.html)

There are 3 stacks that are being setup: (1) VPC stack, (2) RDS, Redis w/ Bastion
and (3) airflow components - web-server, scheduler and worker.

First deploy the VPC and db/redis stacks to setup the database.

cdk bootstrap aws://958237526296/us-east-1
$Env:POSTGRES_PASSWORD=<db password>



