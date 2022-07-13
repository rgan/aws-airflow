import aws_cdk.aws_ec2 as ec2
from aws_cdk import Stack
from aws_cdk.aws_certificatemanager import Certificate
import aws_cdk.aws_ecs as ecs
import aws_cdk.aws_ecs_patterns as ecs_patterns
from aws_cdk.aws_ecr_assets import DockerImageAsset
from aws_cdk.aws_iam import PolicyStatement
from aws_cdk.aws_logs import RetentionDays
from aws_cdk.aws_ssm import StringParameter
from constructs import Construct
import aws_cdk.aws_route53 as route53
import aws_cdk.aws_route53_targets as targets

from airflow_stack.rds_elasticache_stack import RdsElasticacheEfsStack

DB_PORT = 5432
AIRFLOW_WORKER_PORT=8793
REDIS_PORT = 6379

def get_cluster_name(deploy_env):
    return f"AirflowCluster-{deploy_env}"

def get_webserver_service_name(deploy_env):
    return f"AirflowWebserver-{deploy_env}"

def get_webserver_taskdef_family_name(deploy_env):
    return f"AirflowWebTaskDef-{deploy_env}"

def get_scheduler_service_name(deploy_env):
    return f"AirflowSchedulerSvc-{deploy_env}"

def get_scheduler_taskdef_family_name(deploy_env):
    return f"AirflowSchedulerTaskDef-{deploy_env}"

def get_worker_service_name(deploy_env):
    return f"AirflowWorkerSvc-{deploy_env}"

def get_worker_taskdef_family_name(deploy_env):
    return f"AirflowWorkerTaskDef-{deploy_env}"

class AirflowStack(Stack):

    def __init__(self, scope: Construct, id: str, deploy_env: str, vpc:ec2.Vpc, db_redis_stack: RdsElasticacheEfsStack,
                 config: dict, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        self.config = config
        self.deploy_env = deploy_env
        self.db_port = DB_PORT
        # cannot map volumes to Fargate task defs yet - so this is done via Boto3 since CDK does not
        # support it yet: https://github.com/aws/containers-roadmap/issues/825
        #self.efs_file_system_id = db_redis_stack.efs_file_system_id
        cluster_name = get_cluster_name(deploy_env)
        self.cluster = ecs.Cluster(self, cluster_name, cluster_name=cluster_name, vpc=vpc)
        pwd_secret = ecs.Secret.from_ssm_parameter(StringParameter.from_secure_string_parameter_attributes(self, f"dbpwd-{deploy_env}",
                                                                                 version=1, parameter_name="postgres_pwd"))
        self.secrets = {"POSTGRES_PASSWORD": pwd_secret}
        self.vpc = vpc
        environment = {"EXECUTOR": "Celery", "POSTGRES_HOST" : db_redis_stack.db_host,
                       "POSTGRES_PORT": str(self.db_port), "POSTGRES_DB": "airflow", "POSTGRES_USER": self.config["dbadmin"],
                       "REDIS_HOST": db_redis_stack.redis_host,
                       "VISIBILITY_TIMEOUT": str(self.config["celery_broker_visibility_timeout"])}
        image_asset = DockerImageAsset(self, "AirflowImage", directory="build")
        self.image = ecs.ContainerImage.from_docker_image_asset(image_asset)
        # web server - this initializes the db so must happen first
        self.web_service = self.airflow_web_service(environment)
        # https://github.com/aws/aws-cdk/issues/1654
        self.web_service_sg().connections.allow_to_default_port(db_redis_stack.postgres_db, 'allow PG')
        redis_port_info = ec2.Port(protocol=ec2.Protocol.TCP, string_representation="allow to redis",
                               from_port=REDIS_PORT, to_port=REDIS_PORT)
        worker_port_info = ec2.Port(protocol=ec2.Protocol.TCP, string_representation="allow to worker",
                               from_port=AIRFLOW_WORKER_PORT, to_port=AIRFLOW_WORKER_PORT)
        redis_sg = ec2.SecurityGroup.from_security_group_id(self, id=f"Redis-SG-{deploy_env}",
                                                        security_group_id=db_redis_stack.redis.vpc_security_group_ids[0])
        self.web_service_sg().connections.allow_to(redis_sg, redis_port_info, 'allow Redis')
        self.web_service_sg().connections.allow_to_default_port(db_redis_stack.efs_file_system)
        # scheduler
        self.scheduler_service = self.create_scheduler_ecs_service(environment)
        # worker
        self.worker_service = self.worker_service(environment)
        self.scheduler_sg().connections.allow_to_default_port(db_redis_stack.postgres_db, 'allow PG')
        self.scheduler_sg().connections.allow_to(redis_sg, redis_port_info, 'allow Redis')
        self.scheduler_sg().connections.allow_to_default_port(db_redis_stack.efs_file_system)

        self.worker_sg().connections.allow_to_default_port(db_redis_stack.postgres_db, 'allow PG')
        self.worker_sg().connections.allow_to(redis_sg, redis_port_info, 'allow Redis')
        self.worker_sg().connections.allow_to_default_port(db_redis_stack.efs_file_system)
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
        service_name = get_webserver_service_name(self.deploy_env)
        family =  get_webserver_taskdef_family_name(self.deploy_env)
        task_def = ecs.FargateTaskDefinition(self, family, cpu=512, memory_limit_mib=1024, family=family)
        task_def.add_container(f"WebWorker-{self.deploy_env}", image=self.image, environment=environment,
                               secrets=self.secrets, logging=ecs.LogDrivers.aws_logs(stream_prefix=family,
                                                                                     log_retention=RetentionDays.ONE_DAY))
        task_def.default_container.add_port_mappings(ecs.PortMapping(container_port=8080, host_port=8080,
                                                                     protocol=ec2.Protocol.TCP))
        # we want only 1 instance of the web server so when new versions are deployed max_healthy_percent=100
        # you have to manually stop the current version and then it should start a new version - done by deploy task
        lb_security_group = ec2.SecurityGroup(self, f"lb-sec-group-{self.deploy_env}", vpc=self.vpc)
        service = ecs_patterns.ApplicationLoadBalancedFargateService(
            self, service_name,
            cluster=self.cluster,  # Required
            service_name=service_name,
            platform_version=ecs.FargatePlatformVersion.VERSION1_4,
            cpu=512,  # Default is 256
            desired_count=1,  # Default is 1
            task_definition=task_def,
            memory_limit_mib=2048,  # Default is 512
            public_load_balancer=True,
            security_groups=[lb_security_group],
            certificate=Certificate.from_certificate_arn(self, f"lb-cert-{self.deploy_env}",
                                                         certificate_arn=self.config["lb_certificate_arn"]),
            max_healthy_percent=100
        )
        service.target_group.configure_health_check(path="/health")
        # restrict access to the load balancer to only VPN
        lb_security_group.connections.allow_from(ec2.Peer.ipv4(self.config["lb_vpn_addresses"]),
                                                          ec2.Port.tcp(443))
        # configure DNS alias for the load balancer
        route53.ARecord(self, f"lb-record-{self.deploy_env}",
                        zone=route53.HostedZone.from_hosted_zone_attributes(
                            self,
                            f"Zone-{self.deploy_env}",
                            zone_name=f"Zone-{self.deploy_env}",
                            hosted_zone_id=self.config["route53_zone_id"]
                        ),
                        record_name = self.config["lb_dns_name"],
                        target=route53.RecordTarget.from_alias(targets.LoadBalancerTarget(service.load_balancer)))
        return service

    def worker_service(self, environment):
        family = get_worker_taskdef_family_name(self.deploy_env)
        service_name = get_worker_service_name(self.deploy_env)
        service = self.create_service(service_name, family, f"WorkerCont-{self.deploy_env}", environment, "worker",
                                   desired_count=self.config["num_airflow_workers"], cpu=self.config["cpu"],
                                   memory=self.config["memory"], max_healthy_percent=200)
        service.task_definition.add_to_task_role_policy(PolicyStatement(
            resources=["*"],
            actions=["athena:*"]
        ))
        return service

    def create_scheduler_ecs_service(self, environment) -> ecs.FargateService:
        task_family = get_scheduler_taskdef_family_name(self.deploy_env)
        service_name = get_scheduler_service_name(self.deploy_env)
        # we want only 1 instance of the scheduler so when new versions are deployed max_healthy_percent=100
        # you have to manually stop the current version and then it should start a new version - done by deploy task
        return self.create_service(service_name, task_family, f"SchedulerCont-{self.deploy_env}", environment, "scheduler",
                                   desired_count=1, cpu=self.config["cpu"], memory=self.config["memory"],
                                   max_healthy_percent=100)

    def create_service(self, service_name, family, container_name, environment, command, desired_count=1, cpu="512", memory="1024",
                       max_healthy_percent=200):
        worker_task_def = ecs.TaskDefinition(self, family, cpu=cpu, memory_mib=memory,
                                             compatibility=ecs.Compatibility.FARGATE, family=family,
                                             network_mode=ecs.NetworkMode.AWS_VPC)
        worker_task_def.add_container(container_name,
                                      image=self.image,
                                      command=[command], environment=environment,
                                      secrets=self.secrets,
                                      logging=ecs.LogDrivers.aws_logs(stream_prefix=family,
                                                                      log_retention=RetentionDays.ONE_DAY))

        return ecs.FargateService(self, service_name, service_name=service_name,
                                  task_definition=worker_task_def,
                                  cluster=self.cluster, desired_count=desired_count,
                                  platform_version=ecs.FargatePlatformVersion.VERSION1_4, max_healthy_percent=max_healthy_percent)


