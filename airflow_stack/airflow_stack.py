import os

from aws_cdk import core, aws_ec2
from aws_cdk.aws_ec2 import Vpc, Port, Protocol, SecurityGroup
from aws_cdk.aws_ecr_assets import DockerImageAsset
from aws_cdk.aws_iam import PolicyStatement
from aws_cdk.aws_logs import RetentionDays
import aws_cdk.aws_ecs as ecs
import aws_cdk.aws_ecs_patterns as ecs_patterns

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
        # Bug with secrets: https://github.com/aws/aws-cdk/issues/7272
        # secret = ecs.Secret.from_secrets_manager(Secret.from_secret_attributes(self, "pwd",
        #                                                                        secret_arn=config["db_pwd_secret_arn"]),
        #                                                                        field="postgres_pwd")
        #self.secrets = {"POSTGRES_PASSWORD" : secret}
        environment = {"EXECUTOR": "Celery", "POSTGRES_HOST" : db_redis_stack.db_host,
                       "POSTGRES_PORT": str(self.db_port), "POSTGRES_DB": "airflow", "POSTGRES_USER": self.config["dbadmin"],
                       # have to pass like this due to issues above
                       "POSTGRES_PASSWORD": os.environ["POSTGRES_PASSWORD"],
                       "REDIS_HOST": db_redis_stack.redis_host}
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
        redis_sg = SecurityGroup.from_security_group_id(self, id=f"Redis-SG-{deploy_env}",
                                                        security_group_id=db_redis_stack.redis.vpc_security_group_ids[0])
        self.web_service_sg().connections.allow_to(redis_sg, redis_port_info, 'allow Redis')
        # scheduler
        self.scheduler_service = self.create_scheduler_ecs_service(environment)
        # worker
        self.worker_service = self.worker_service(environment)
        self.scheduler_sg().connections.allow_to_default_port(db_redis_stack.postgres_db, 'allow PG')
        self.scheduler_sg().connections.allow_to(redis_sg, redis_port_info, 'allow Redis')

        self.worker_sg().connections.allow_to_default_port(db_redis_stack.postgres_db, 'allow PG')
        self.worker_sg().connections.allow_to(redis_sg, redis_port_info, 'allow Redis')
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
        service = ecs_patterns.ApplicationLoadBalancedFargateService(self, service_name,
                                                                         cluster=self.cluster,  # Required
                                                                         service_name=service_name,
                                                                         platform_version=ecs.FargatePlatformVersion.VERSION1_4,
                                                                         cpu=512,  # Default is 256
                                                                         desired_count=1,  # Default is 1
                                                                         task_image_options=ecs_patterns.ApplicationLoadBalancedTaskImageOptions(
                                                                             image=self.image,
                                                                              log_driver=ecs.LogDrivers.aws_logs(
                                                                                 stream_prefix=f"Worker",
                                                                                 log_retention=RetentionDays.ONE_DAY),
                                                                             container_port=8080,
                                                                             #secrets=self.secrets,
                                                                             family= f"AirflowWebTaskDef-{self.deploy_env}",
                                                                             environment=environment),
                                                                         memory_limit_mib=2048,  # Default is 512
                                                                         public_load_balancer=True
                                                                         )
        service.target_group.configure_health_check(path="/health")
        service.task_definition.add_to_execution_role_policy(PolicyStatement(resources=[self.config["db_pwd_secret_arn"]],
                                                     actions=["secretsmanager:GetSecretValue"]))
        return service

    def worker_service(self, environment):
        family = f"WorkerTaskDef-{self.deploy_env}"
        worker_task_def = ecs.TaskDefinition(self, family, cpu="512", memory_mib="1024",
                                             compatibility=ecs.Compatibility.FARGATE, family=family)
        worker_task_def.add_container(f"WorkerCont-{self.deploy_env}",
                                      image=self.image,
                                      command=["worker"], environment=environment,
                                      #secrets=self.secrets,
                                      logging=ecs.LogDrivers.aws_logs(stream_prefix=f"Worker",
                                                                      log_retention=RetentionDays.ONE_DAY))
        worker_task_def.add_to_execution_role_policy(PolicyStatement(resources=[self.config["db_pwd_secret_arn"]],
                            actions=["secretsmanager:GetSecretValue"]))
        return ecs.FargateService(self, f"AirflowWorker-{self.deploy_env}", task_definition=worker_task_def,
                                  cluster=self.cluster, desired_count=self.config["num_airflow_workers"],
                                  platform_version=ecs.FargatePlatformVersion.VERSION1_4)

    def create_scheduler_ecs_service(self, environment) -> ecs.FargateService:
        task_family = f"SchedulerTaskDef-{self.deploy_env}"
        scheduler_task_def = ecs.TaskDefinition(self, f"SchedulerTaskDef-{self.deploy_env}", cpu="512",
                                                memory_mib="1024", family=task_family,
                                                compatibility=ecs.Compatibility.FARGATE)
        scheduler_task_def.add_container(f"SchedulerCont-{self.deploy_env}",
                                         image=self.image,
                                         command=["scheduler"], environment=environment,
                                         #secrets=self.secrets,
                                         logging=ecs.LogDrivers.aws_logs(stream_prefix=f"Scheduler",
                                                                         log_retention=RetentionDays.ONE_DAY))
        scheduler_task_def.add_to_execution_role_policy(PolicyStatement(resources=[self.config["db_pwd_secret_arn"]],
                            actions=["secretsmanager:GetSecretValue"]))
        return ecs.FargateService(self, f"AirflowSch-{self.deploy_env}", task_definition=scheduler_task_def,
                           cluster=self.cluster, desired_count=1, platform_version=ecs.FargatePlatformVersion.VERSION1_4)