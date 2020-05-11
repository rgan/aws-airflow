import os

from aws_cdk import core, aws_ec2
from aws_cdk.aws_ec2 import Vpc, Port, Protocol, SecurityGroup
from aws_cdk.aws_logs import RetentionDays
import aws_cdk.aws_ecs as ecs
import aws_cdk.aws_ecs_patterns as ecs_patterns
from airflow_stack.rds_elasticache_stack import RdsElasticacheStack
from airflow_stack.secret_value import SecretValueFix

DB_PORT = 5432
REDIS_PORT = 6379

DOCKER_AIRFLOW = "puckel/docker-airflow"

class AirflowStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, deploy_env: str, vpc:aws_ec2.Vpc, db_redis_stack: RdsElasticacheStack,
                 config: dict, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        self.config = config
        self.deploy_env = deploy_env
        self.db_port = DB_PORT
        self.vpc = Vpc(self, f"AirflowVPC-{deploy_env}", cidr="10.0.0.0/16", max_azs=2)
        self.cluster = ecs.Cluster(self, "AirflowCluster", vpc=vpc)
        db_passwd = SecretValueFix(config["db_pwd_secret_arn"], "postgres_pwd")
        environment = {"EXECUTOR": "Celery", "POSTGRES_HOST" : db_redis_stack.db_host,
                       "POSTGRES_PORT": str(self.db_port), "POSTGRES_DB": "airflow", "POSTGRES_USER": self.config["dbadmin"],
                       "POSTGRES_PASSWORD": db_passwd.to_string(), "REDIS_HOST": db_redis_stack.redis_host}
        # web server - this initializes the db so must happen first
        self.web_service = self.airflow_web_service(environment)
        # https://github.com/aws/aws-cdk/issues/1654
        self.web_service_sg().connections.allow_to_default_port(db_redis_stack.postgres_db, 'allow PG')
        redis_port_info = Port(protocol=Protocol.TCP, string_representation="allow to redis",
                               from_port=db_redis_stack.redis.port, to_port=db_redis_stack.redis.port)
        redis_sg = SecurityGroup.from_security_group_id(self, id=f"Redis-SG-{deploy_env}",
                                                        security_group_id=db_redis_stack.redis.vpc_security_group_ids[0])
        self.web_service_sg().connections.allow_to(redis_sg, redis_port_info, 'allow Redis')
        # scheduler
        # self.scheduler_service = self.create_scheduler_ecs_service(environment)
        # self.scheduler_sg().connections.allow_to_default_port(db_redis_stack.postgres_db, 'allow PG')
        # self.scheduler_sg().connections.allow_to(redis_sg, redis_port_info, 'allow Redis')
        # # worker
        # self.worker_service = self.worker_service(environment)
        # self.worker_sg().connections.allow_to_default_port(db_redis_stack.postgres_db, 'allow PG')
        # self.worker_sg().connections.allow_to(redis_sg, redis_port_info, 'allow Redis')

    def web_service_sg(self):
        return self.web_service.service.connections.security_groups[0]

    def scheduler_sg(self):
        return self.scheduler_service.connections.security_groups[0]

    def worker_sg(self):
        return self.worker_service.connections.security_groups[0]

    def airflow_web_service(self, environment):
        return ecs_patterns.ApplicationLoadBalancedFargateService(self, f"AirflowWebserver-{self.deploy_env}",
                                                                         cluster=self.cluster,  # Required
                                                                         cpu=512,  # Default is 256
                                                                         desired_count=1,  # Default is 1
                                                                         task_image_options=ecs_patterns.ApplicationLoadBalancedTaskImageOptions(
                                                                             image=ecs.ContainerImage.from_registry(
                                                                                 DOCKER_AIRFLOW),
                                                                             log_driver=ecs.LogDrivers.aws_logs(
                                                                                 stream_prefix=f"Worker",
                                                                                 log_retention=RetentionDays.ONE_DAY),
                                                                             container_port=8080,
                                                                             environment=environment),
                                                                         memory_limit_mib=2048,  # Default is 512
                                                                         public_load_balancer=True
                                                                         )

    def worker_service(self, environment):
        worker_task_def = ecs.TaskDefinition(self, f"WorkerTaskDef-{self.deploy_env}", cpu="512", memory_mib="1024",
                                             compatibility=ecs.Compatibility.FARGATE)
        worker_task_def.add_container(f"WorkerCont-{self.deploy_env}",
                                      image=ecs.ContainerImage.from_registry(DOCKER_AIRFLOW),
                                      command=["worker"], environment=environment,
                                      logging=ecs.LogDrivers.aws_logs(stream_prefix=f"Worker",
                                                                      log_retention=RetentionDays.ONE_DAY))
        return ecs.FargateService(self, f"AirflowWorker-{self.deploy_env}", task_definition=worker_task_def,
                           cluster=self.cluster, desired_count=1)

    def create_scheduler_ecs_service(self, environment) -> ecs.FargateService:
        scheduler_task_def = ecs.TaskDefinition(self, f"SchedulerTaskDef-{self.deploy_env}", cpu="512",
                                                memory_mib="1024",
                                                compatibility=ecs.Compatibility.FARGATE)
        scheduler_task_def.add_container(f"SchedulerCont-{self.deploy_env}",
                                         image=ecs.ContainerImage.from_registry(DOCKER_AIRFLOW),
                                         command=["scheduler"], environment=environment,
                                         logging=ecs.LogDrivers.aws_logs(stream_prefix=f"Scheduler",
                                                                         log_retention=RetentionDays.ONE_DAY))
        return ecs.FargateService(self, f"AirflowSch-{self.deploy_env}", task_definition=scheduler_task_def,
                           cluster=self.cluster, desired_count=1)