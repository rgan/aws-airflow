import os
from collections import Mapping

from aws_cdk import core, aws_ec2
from aws_cdk.aws_ec2 import Vpc, Port, Protocol, SecurityGroup
from aws_cdk.aws_ecr_assets import DockerImageAsset
from aws_cdk.aws_iam import PolicyStatement
from aws_cdk.aws_logs import RetentionDays
import aws_cdk.aws_ecs as ecs
import aws_cdk.aws_ecs_patterns as ecs_patterns
from aws_cdk.aws_secretsmanager import Secret
from aws_cdk.aws_ssm import StringParameter

from airflow_stack.rds_elasticache_stack import RdsElasticacheEfsStack


DB_PORT = 5432
AIRFLOW_WORKER_PORT=8793
REDIS_PORT = 6379

class AirflowStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, deploy_env: str, vpc:aws_ec2.Vpc, db_redis_stack: RdsElasticacheEfsStack,
                 config: dict, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        self.config = config
        self.deploy_env = deploy_env
        self.db_port = DB_PORT
        # cannot map volumes to Fargate task defs yet - so this is done via Boto3 since CDK does not
        # support it yet: https://github.com/aws/containers-roadmap/issues/825
        self.efs_file_system_id = db_redis_stack.efs_file_system_id
        self.cluster = ecs.Cluster(self, "AirflowCluster", vpc=vpc)
        pwd_secret = ecs.Secret.from_ssm_parameter(StringParameter.from_secure_string_parameter_attributes(self, f"dbpwd-{deploy_env}",
                                                                                 version=1, parameter_name="postgres_pwd"))
        ssh_key_secret = ecs.Secret.from_ssm_parameter(StringParameter.from_secure_string_parameter_attributes(self, f"ssh_public_key-{deploy_env}",
                                                                                 version=1, parameter_name="ssh_public_key"))
        self.secrets = {"POSTGRES_PASSWORD": pwd_secret, "SSH_PUBLIC_KEY": ssh_key_secret}
        environment = {"EXECUTOR": "Celery", "POSTGRES_HOST" : db_redis_stack.db_host,
                       "POSTGRES_PORT": str(self.db_port), "POSTGRES_DB": "airflow", "POSTGRES_USER": self.config["dbadmin"],
                       "REDIS_HOST": db_redis_stack.redis_host,
                       "VISIBILITY_TIMEOUT": str(self.config["celery_broker_visibility_timeout"])}
        image_asset = DockerImageAsset(self, "AirflowImage", directory="build",
                                       repository_name=config["ecr_repo_name"])
        self.image = ecs.ContainerImage.from_docker_image_asset(image_asset)
        # web server - this initializes the db so must happen first
        self.web_service = self.airflow_web_service(environment)
        # https://github.com/aws/aws-cdk/issues/1654
        self.web_service_sg().connections.allow_to_default_port(db_redis_stack.postgres_db, 'allow PG')
        redis_port_info = Port(protocol=Protocol.TCP, string_representation="allow to redis",
                               from_port=REDIS_PORT, to_port=REDIS_PORT)
        worker_port_info = Port(protocol=Protocol.TCP, string_representation="allow to worker",
                               from_port=AIRFLOW_WORKER_PORT, to_port=AIRFLOW_WORKER_PORT)
        ssh_port_info = Port(protocol=Protocol.TCP, string_representation="allow ssh",
                             from_port=22, to_port=22)
        redis_sg = SecurityGroup.from_security_group_id(self, id=f"Redis-SG-{deploy_env}",
                                                        security_group_id=db_redis_stack.redis.vpc_security_group_ids[0])
        bastion_sg = db_redis_stack.bastion.connections.security_groups[0]
        self.web_service_sg().connections.allow_to(redis_sg, redis_port_info, 'allow Redis')
        self.web_service_sg().connections.allow_to(bastion_sg, ssh_port_info, 'allow ssh')
        # scheduler
        self.scheduler_service = self.create_scheduler_ecs_service(environment)
        # worker
        self.worker_service = self.worker_service(environment)
        self.scheduler_sg().connections.allow_to_default_port(db_redis_stack.postgres_db, 'allow PG')
        self.scheduler_sg().connections.allow_to(redis_sg, redis_port_info, 'allow Redis')
        self.scheduler_sg().connections.allow_to(bastion_sg, ssh_port_info, 'allow ssh')

        self.worker_sg().connections.allow_to_default_port(db_redis_stack.postgres_db, 'allow PG')
        db_redis_stack.bastion.connections.allow_to(redis_sg, redis_port_info, 'allow Redis')
        self.worker_sg().connections.allow_to(bastion_sg, ssh_port_info, 'allow ssh')
        # When you start an airflow worker, airflow starts a tiny web server
        # subprocess to serve the workers local log files to the airflow main
        # web server, who then builds pages and sends them to users. This defines
        # the port on which the logs are served. It needs to be unused, and open
        # visible from the main web server to connect into the workers.
        self.web_service_sg().connections.allow_to(self.worker_sg(), worker_port_info, 'web service to worker')

    def web_service_sg(self):
        return self.web_service.service.connections.security_groups[0]

    def scheduler_sg(self):
        return self.scheduler_service.connections.security_groups[0]

    def worker_sg(self):
        return self.worker_service.connections.security_groups[0]

    def airflow_web_service(self, environment):
        service_name = f"AirflowWebserver-{self.deploy_env}"
        family =  f"AirflowWebTaskDef-{self.deploy_env}"
        task_def = ecs.FargateTaskDefinition(self, family, cpu=512, memory_limit_mib=1024,
                                             family=family)
        task_def.add_container(f"WebWorker-{self.deploy_env}",
                                      image=self.image,
                                      environment=environment,
                                      secrets=self.secrets,
                                      logging=ecs.LogDrivers.aws_logs(stream_prefix=f"Worker",
                                                                      log_retention=RetentionDays.ONE_DAY))
        task_def.default_container.add_port_mappings(ecs.PortMapping(container_port=22,
                                                                                    host_port=22,
                                                                                    protocol=Protocol.TCP))
        task_def.default_container.add_port_mappings(ecs.PortMapping(container_port=8080,
                                                                                    host_port=8080,
                                                                                    protocol=Protocol.TCP))
        service = ecs_patterns.ApplicationLoadBalancedFargateService(self, service_name,
                                                                         cluster=self.cluster,  # Required
                                                                         service_name=service_name,
                                                                         assign_public_ip=True,
                                                                         platform_version=ecs.FargatePlatformVersion.VERSION1_4,
                                                                         cpu=512,  # Default is 256
                                                                         desired_count=1,  # Default is 1
                                                                         task_definition=task_def,
                                                                         memory_limit_mib=2048,  # Default is 512
                                                                         public_load_balancer=True
                                                                         )
        service.target_group.configure_health_check(path="/health")
        return service

    def worker_service(self, environment):
        family = f"WorkerTaskDef-{self.deploy_env}"
        worker_task_def = ecs.TaskDefinition(self, family, cpu="512", memory_mib="1024",
                                             compatibility=ecs.Compatibility.FARGATE, family=family,
                                             network_mode=ecs.NetworkMode.AWS_VPC)
        worker_task_def.add_container(f"WorkerCont-{self.deploy_env}",
                                      image=self.image,
                                      command=["worker"], environment=environment,
                                      secrets=self.secrets,
                                      logging=ecs.LogDrivers.aws_logs(stream_prefix=f"Worker",
                                                                      log_retention=RetentionDays.ONE_DAY))
        worker_task_def.default_container.add_port_mappings(ecs.PortMapping(container_port=22,
                                                                                              host_port=22,
                                                                                              protocol=Protocol.TCP))
        worker_task_def.add_to_execution_role_policy(PolicyStatement(resources=[self.config["db_pwd_secret_arn"]],
                            actions=["secretsmanager:GetSecretValue"]))
        return ecs.FargateService(self, f"AirflowWorker-{self.deploy_env}", task_definition=worker_task_def,
                                  cluster=self.cluster, desired_count=self.config["num_airflow_workers"],
                                  platform_version=ecs.FargatePlatformVersion.VERSION1_4)

    def create_scheduler_ecs_service(self, environment) -> ecs.FargateService:
        task_family = f"SchedulerTaskDef-{self.deploy_env}"
        scheduler_task_def = ecs.TaskDefinition(self, f"SchedulerTaskDef-{self.deploy_env}", cpu="512",
                                                memory_mib="1024", family=task_family,
                                                compatibility=ecs.Compatibility.FARGATE,
                                                network_mode=ecs.NetworkMode.AWS_VPC)
        scheduler_task_def.add_container(f"SchedulerCont-{self.deploy_env}",
                                         image=self.image,
                                         command=["scheduler"], environment=environment,
                                         secrets=self.secrets,
                                         logging=ecs.LogDrivers.aws_logs(stream_prefix=f"Scheduler",
                                                                         log_retention=RetentionDays.ONE_DAY))
        scheduler_task_def.add_to_execution_role_policy(PolicyStatement(resources=[self.config["db_pwd_secret_arn"]],
                            actions=["secretsmanager:GetSecretValue"]))
        scheduler_task_def.default_container.add_port_mappings(ecs.PortMapping(container_port=22,
                                                                                              host_port=22,
                                                                                              protocol=Protocol.TCP))
        return ecs.FargateService(self, f"AirflowSch-{self.deploy_env}", task_definition=scheduler_task_def,
                           cluster=self.cluster, desired_count=1, platform_version=ecs.FargatePlatformVersion.VERSION1_4)