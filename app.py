#!/usr/bin/env python3
import json
import os

from aws_cdk import App, Environment

from airflow_stack.airflow_stack import AirflowStack
from airflow_stack.rds_elasticache_stack import RdsElasticacheEfsStack
from airflow_stack.vpc_stack import VpcStack

app = App()
deploy_env = os.environ.get("ENV", "dev")
config = json.loads(open("conf/{0}/config.json".format(deploy_env)).read())
us_east_env = Environment(account=config["account_id"], region="us-east-1")

vpc_stack = VpcStack(app, f"vpc-{deploy_env}", deploy_env, config, env=us_east_env)
db_redis_stack = RdsElasticacheEfsStack(app, f"airflow-db-{deploy_env}", deploy_env, vpc_stack.get_vpc(), config, env=us_east_env)
db_redis_stack.add_dependency(vpc_stack)
airflow_stack = AirflowStack(app, f"airflow-{deploy_env}", deploy_env,  vpc_stack.get_vpc(), db_redis_stack,
             config, env=us_east_env)
airflow_stack.add_dependency(db_redis_stack)

app.synth()
