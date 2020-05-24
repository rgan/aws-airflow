import shutil

import boto3
from invoke import task
import os

@task
def prepare_docker_build(context, env):
    build_dir = "build"
    if not os.path.exists(build_dir): os.mkdir(build_dir)
    for f in ["Dockerfile", f"conf/{env}/airflow.cfg"]:
        shutil.copy(f, build_dir)

@task
def deploy_vpc_db(context, env):
    context.run(f"cdk deploy --require-approval never vpc-{env} airflow-db-{env}")

@task
def deploy_airflow(context, env):
    prepare_docker_build(context, env)
    context.run(f"cdk deploy --require-approval never airflow-{env}")

# cannot map volumes to Fargate task defs yet - so this is done via Boto3 since CDK does not
 # support it yet: https://github.com/aws/containers-roadmap/issues/825
@task
def setup_efs(context, deploy_env, file_system_id):
    client = boto3.client('ecs', region_name='us-east-1')
    services_and_tasks = [
        (f"AirflowWebserver-{deploy_env}",  f"AirflowWebTaskDef-{deploy_env}"),
        (f"AirflowSchedulerSvc-{deploy_env}", f"SchedulerTaskDef-{deploy_env}"),
        (f"AirflowWorkerSvc-{deploy_env}", f"WorkerTaskDef-{deploy_env}")
    ]
    for service_name, task_name in services_and_tasks:
        update_service_task_def_with_efs_volume(client, file_system_id, service_name, task_name, '/mnt/efs')


def update_service_task_def_with_efs_volume(client, file_system_id, service_name, task_name, root):
    print(task_name)
    response = client.describe_task_definition(taskDefinition=task_name)
    task_def = response["taskDefinition"]
    print(task_def)
    volumes = [
        {
            'name': "efsvolume",
            'efsVolumeConfiguration': {
                'fileSystemId': file_system_id,
                'transitEncryption': 'DISABLED'
            }
        },
    ]
    mount_points = [ {"sourceVolume": "efsvolume", "containerPath": "/mnt/efs"}]
    task_def_arn = task_def["taskDefinitionArn"]
    add_volumes(task_def, volumes, mount_points)
    client.deregister_task_definition(taskDefinition=task_def_arn)
    response = client.register_task_definition(**task_def)
    updated_task_def_arn = response["taskDefinition"]["taskDefinitionArn"]
    client.update_service(
        service=service_name,
        taskDefinition=updated_task_def_arn,
    )


def add_volumes(task_def, volumes, mount_points):
    task_def["volumes"] = volumes
    del task_def["taskDefinitionArn"]
    del task_def["revision"]
    del task_def["status"]
    del task_def["requiresAttributes"]
    compatibilities = task_def["compatibilities"]
    del task_def["compatibilities"]
    task_def["requiresCompatibilities"] = compatibilities
    task_def["containerDefinitions"][0]["mountPoints"] = mount_points

