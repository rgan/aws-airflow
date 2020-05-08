#!/usr/bin/env python3
import json
import os

from aws_cdk import core

from airflow_stack.airflow_stack import AirflowStack

app = core.App()
deploy_env = os.environ.get("ENV", "dev")
config = json.loads(open("conf/{0}/config.json".format(deploy_env)).read())
us_east_env = core.Environment(account=config["account_id"], region="us-east-1")

stack = AirflowStack(app, "airflow-{0}".format(deploy_env), deploy_env, config, env=us_east_env)

app.synth()
