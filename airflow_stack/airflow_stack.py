import os

from aws_cdk import core
from aws_cdk.aws_ec2 import Vpc, SecurityGroup, Port, Protocol, InstanceType
from aws_cdk.aws_logs import RetentionDays
from aws_cdk.aws_rds import DatabaseInstance, DatabaseInstanceEngine
import aws_cdk.aws_ecs as ecs
import aws_cdk.aws_ecs_patterns as ecs_patterns
import aws_cdk.aws_elasticache as elasticache
from aws_cdk.core import SecretValue

DB_PORT = 5432

REDIS_PORT = 6379

DOCKER_AIRFLOW = "puckel/docker-airflow"

class AirflowStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, deploy_env: str, config: dict, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        self.config = config
        self.deploy_env = deploy_env
        vpc = Vpc(self, f"AirflowVPC-{deploy_env}", cidr="10.0.0.0/16", max_azs=2)
        db_security_group = SecurityGroup(self, f"AirflowPostgresSG-{deploy_env}", vpc=vpc)
        #db_pwd_secret = SecretValue.secrets_manager(secret_id=config["db_pwd_secret_arn"])
        postgres_db = DatabaseInstance(self, f"AirflowPostgresDb-{deploy_env}", engine=DatabaseInstanceEngine.POSTGRES,
                                       multi_az=self.config["mutli_az_db"],
                                       enable_performance_insights=self.config["db_enable_performance_insights"],
                                       allocated_storage=10,
                                       instance_class=InstanceType("t2.micro"),
                                       master_username=self.config["dbadmin"],
                                       vpc=vpc, # default placement is private subnets
                                       security_groups=[db_security_group])
        redis_security_group = SecurityGroup(self, f"AirflowRedisSG-{deploy_env}", vpc=vpc)
        redis_subnet_group_name = f"AirflowRedisSubnetGrp-{deploy_env}"
        redis_subnet_group = elasticache.CfnSubnetGroup(self, redis_subnet_group_name,
                                                        subnet_ids=[s.subnet_id for s in vpc.private_subnets],
                                                        description="Airflow Redis Cache Subnet Group",
                                                        cache_subnet_group_name=redis_subnet_group_name)
        redis = elasticache.CfnCacheCluster(self, f"AirflowRedis-{deploy_env}", cache_node_type=config["cache_node_type"],
                                    engine="redis", num_cache_nodes=config["num_cache_nodes"],
                                    az_mode=config["cache_az_mode"], vpc_security_group_ids=[redis_security_group.security_group_id],
                                            cache_subnet_group_name=redis_subnet_group_name)
        redis.add_depends_on(redis_subnet_group)
        cluster = ecs.Cluster(self, "AirflowCluster", vpc=vpc)
        environment = {"EXECUTOR": "Celery", "POSTGRES_HOST" : postgres_db.db_instance_endpoint_address,
                       "POSTGRES_PORT": str(DB_PORT), "POSTGRES_DB": "airflow", "POSTGRES_USER": self.config["dbadmin"],
                       "POSTGRES_PASSWORD": os.environ["POSTGRES_PWD"], "REDIS_HOST": redis.attr_redis_endpoint_address}
        # scheduler
        scheduler_task_def = ecs.TaskDefinition(self, f"SchedulerTaskDef-{self.deploy_env}", cpu="512", memory_mib="1024",
                                                compatibility=ecs.Compatibility.FARGATE)
        scheduler_task_def.add_container(f"SchedulerCont-{self.deploy_env}", image=ecs.ContainerImage.from_registry(DOCKER_AIRFLOW),
                                         command=["scheduler"], environment=environment,
                                         logging=ecs.LogDrivers.aws_logs(stream_prefix=f"Scheduler", log_retention=RetentionDays.ONE_DAY))
        scheduler_security_group = SecurityGroup(self, f"AirflowSchedulerSG-{self.deploy_env}", vpc=vpc)
        redis_security_group.add_ingress_rule(peer=scheduler_security_group,
                                              connection=Port(protocol=Protocol.TCP, from_port=REDIS_PORT,
                                                              to_port=REDIS_PORT, string_representation="allow scheduler to redis"))
        db_security_group.add_ingress_rule(peer=scheduler_security_group,
                                           connection=Port(protocol=Protocol.TCP, from_port=DB_PORT,
                                                           to_port=DB_PORT, string_representation="allow scheduler to db"))
        ecs.FargateService(self, f"AirflowSch-{self.deploy_env}", task_definition=scheduler_task_def,
                           cluster=cluster, security_group=scheduler_security_group, desired_count=1)
        # worker
        worker_task_def = ecs.TaskDefinition(self, f"WorkerTaskDef-{self.deploy_env}", cpu="512", memory_mib="1024",
                                             compatibility=ecs.Compatibility.FARGATE)
        worker_task_def.add_container(f"WorkerCont-{self.deploy_env}", image=ecs.ContainerImage.from_registry(DOCKER_AIRFLOW),
                                      command=["worker"], environment=environment,
                                      logging=ecs.LogDrivers.aws_logs(stream_prefix=f"Worker", log_retention=RetentionDays.ONE_DAY))
        worker_security_group = SecurityGroup(self, f"AirflowWorkerSG-{self.deploy_env}", vpc=vpc)
        redis_security_group.add_ingress_rule(peer=worker_security_group,
                                              connection=Port(protocol=Protocol.TCP, from_port=REDIS_PORT,
                                                              to_port=REDIS_PORT, string_representation="allow worker to redis"))
        db_security_group.add_ingress_rule(peer=worker_security_group,
                                           connection=Port(protocol=Protocol.TCP, from_port=DB_PORT,
                                                           to_port=DB_PORT, string_representation="allow worker to db"))
        ecs.FargateService(self, f"AirflowWorker-{self.deploy_env}", task_definition=worker_task_def,
                           cluster=cluster, security_group=worker_security_group, desired_count=1)
        # web server
        web_service = ecs_patterns.ApplicationLoadBalancedFargateService(self, f"AirflowWebserver-{self.deploy_env}",
                                                            cluster=cluster,  # Required
                                                            cpu=512,  # Default is 256
                                                            desired_count=1,  # Default is 1
                                                            task_image_options=ecs_patterns.ApplicationLoadBalancedTaskImageOptions(
                                                                image=ecs.ContainerImage.from_registry(DOCKER_AIRFLOW),
                                                                log_driver=ecs.LogDrivers.aws_logs(stream_prefix=f"Worker",
                                                                                                log_retention=RetentionDays.ONE_DAY),
                                                                container_port=8080, environment=environment),
                                                            memory_limit_mib=2048,  # Default is 512
                                                            public_load_balancer=True
                                                           )
        # allow access from web server to db
        web_service_sg = web_service.service.connections.security_groups[0]
        db_security_group.add_ingress_rule(peer=web_service_sg,
                                           connection=Port(protocol=Protocol.TCP, from_port=DB_PORT,
                                                           to_port=DB_PORT, string_representation="allow web server to db"))
        redis_security_group.add_ingress_rule(peer=web_service_sg,
                                              connection=Port(protocol=Protocol.TCP, from_port=REDIS_PORT,
                                                              to_port=REDIS_PORT,
                                                              string_representation="allow web server to redis"))