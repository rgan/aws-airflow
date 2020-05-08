from aws_cdk import core, aws_eks, aws_iam
from aws_cdk.aws_ec2 import Vpc, SecurityGroup, SubnetType, Port, Protocol
from aws_cdk.aws_eks import KubernetesResource
from aws_cdk.aws_iam import Role
from aws_cdk.aws_rds import DatabaseInstance, DatabaseInstanceEngine
import aws_cdk.aws_ecs as ecs
import aws_cdk.aws_ecs_patterns as ecs_patterns
import aws_cdk.aws_elasticache as elasticache

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
        postgres_db = DatabaseInstance(self, f"AirflowPostgresDb-{deploy_env}", engine=DatabaseInstanceEngine.POSTGRES,
                                       multi_az=self.config["mutli_az_db"],
                                       enable_performance_insights=self.config["db_enable_performance_insights"],
                                       allocated_storage=10,
                                       instance_class="db.t2.micro",
                                       master_username=self.config["dbadmin"],
                                       master_user_password=self.config["db_pwd_secret"],
                                       vpc=vpc, # default placement is private subnets
                                       security_groups=[db_security_group])
        redis_security_group = SecurityGroup(self, f"AirflowRedisSG-{deploy_env}", vpc=vpc)
        redis = elasticache.CfnCacheCluster(self, f"AirflowRedis-{deploy_env}", cache_node_type=config["cache_node_type"],
                                    engine="redis", num_cache_nodes=config["num_cache_nodes"],
                                    az_mode=config["cache_az_mode"], vpc_security_group_ids=[redis_security_group])
        cluster = ecs.Cluster(self, "AirflowCluster", vpc=vpc)
        environment = {"EXECUTOR": "CeleryExecutor", "POSTGRES_HOST" : postgres_db.dbInstanceEndpointAddress,
                       "POSTGRES_PORT": DB_PORT, "POSTGRES_DB": "airflow", "POSTGRES_USER": self.config["dbadmin"],
                       "POSTGRES_PASSWORD": "", "REDIS_HOST": redis.attr_redis_endpoint_address}
        # scheduler
        scheduler_task_def = ecs.TaskDefinition(self, f"SchedulerTaskDef-{deploy_env}", cpu="512", memory_mib="1024",
                                                compatibility=ecs.Compatibility.FARGATE)
        scheduler_task_def.add_container(image=ecs.ContainerImage.from_registry(DOCKER_AIRFLOW),
                                         command=["scheduler"], environment=environment)
        scheduler_security_group = SecurityGroup(self, f"AirflowSchedulerSG-{deploy_env}", vpc=vpc)
        redis_security_group.add_ingress_rule(peer=scheduler_security_group,
                                              connection=Port(protocol=Protocol.TCP, from_port=REDIS_PORT,
                                                              to_port=REDIS_PORT, string_representation="allow scheduler to redis"))
        db_security_group.add_ingress_rule(peer=scheduler_security_group,
                                           connection=Port(protocol=Protocol.TCP, from_port=DB_PORT,
                                                           to_port=DB_PORT, string_representation="allow scheduler to db"))
        ecs.FargateService(self, f"AirflowSch-{deploy_env}", task_definition=scheduler_task_def,
                           cluster=cluster, security_group=scheduler_security_group, desired_count=1)
        # worker
        worker_task_def = ecs.TaskDefinition(self, f"WorkerTaskDef-{deploy_env}", cpu="512", memory_mib="1024",
                                             compatibility=ecs.Compatibility.FARGATE)
        worker_task_def.add_container(image=ecs.ContainerImage.from_registry(DOCKER_AIRFLOW),
                                      command=["worker"], environment=environment)
        worker_security_group = SecurityGroup(self, f"AirflowWorkerSG-{deploy_env}", vpc=vpc)
        redis_security_group.add_ingress_rule(peer=worker_security_group,
                                              connection=Port(protocol=Protocol.TCP, from_port=REDIS_PORT,
                                                              to_port=REDIS_PORT, string_representation="allow worker to redis"))
        db_security_group.add_ingress_rule(peer=worker_security_group,
                                           connection=Port(protocol=Protocol.TCP, from_port=DB_PORT,
                                                           to_port=DB_PORT, string_representation="allow worker to db"))
        ecs.FargateService(self, f"AirflowWorker-{deploy_env}", task_definition=worker_task_def,
                           cluster=cluster, security_group=worker_security_group, desired_count=1)
        # web server
        web_service = ecs_patterns.ApplicationLoadBalancedFargateService(self, f"AirflowWebserver-{deploy_env}",
                                                            cluster=cluster,  # Required
                                                            cpu=512,  # Default is 256
                                                            desired_count=1,  # Default is 1
                                                            task_image_options=ecs_patterns.ApplicationLoadBalancedTaskImageOptions(
                                                                image=ecs.ContainerImage.from_registry(DOCKER_AIRFLOW),
                                                                enable_logging=True,
                                                                container_port=8080, environment=environment),
                                                            memory_limit_mib=2048,  # Default is 512
                                                            public_load_balancer=True
                                                           )
        # allow access from web server to db
        db_security_group.add_ingress_rule(peer=web_service.service.connections.security_groups[0],
                                           connection=Port(protocol=Protocol.TCP, from_port=DB_PORT,
                                                           to_port=DB_PORT, string_representation="allow web server to db"))