import boto3
from invoke import task

# cannot map volumes to Fargate task defs yet - so this is done via Boto3 since CDK does not
 # support it yet: https://github.com/aws/containers-roadmap/issues/825
@task
def setup_efs(context, deploy_env, file_system_id):
    client = boto3.client('ecs', region_name='us-east-1')
    service_name = f"AirflowWebserver-{deploy_env}"
    task_name = f"AirflowWebTaskDef-{deploy_env}"
    update_service_task_def_with_efs_volume(client, file_system_id, service_name, task_name, '/mnt/efs')


def update_service_task_def_with_efs_volume(client, file_system_id, service_name, task_name, root):
    response = client.describe_task_definition(
        taskDefinition=task_name,
    )
    task_def = response["taskDefinition"]
    print(task_def)
    volumes = [
        {
            'name': "efsvolume",
            'efsVolumeConfiguration': {
                'fileSystemId': file_system_id,
                'rootDirectory': root,
                'transitEncryption': 'DISABLED'
            }
        },
    ]
    add_volumes(task_def, volumes)
    client.deregister_task_definition(task_def["taskDefinitionArn"])
    response = client.register_task_definition(**task_def)
    updated_task_def_arn = response["taskDefinition"]["taskDefinitionArn"]
    client.update_service(
        service=service_name,
        taskDefinition=updated_task_def_arn,
    )


def add_volumes(task_def, volumes):
    task_def["volumes"] = volumes
    del task_def["taskDefinitionArn"]
    del task_def["revision"]
    del task_def["status"]
    del task_def["requiresAttributes"]
    compatibilities = task_def["compatibilities"]
    del task_def["compatibilities"]
    task_def["requiresCompatibilities"] = compatibilities

