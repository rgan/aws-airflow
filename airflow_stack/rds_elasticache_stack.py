from aws_cdk import core, aws_ec2
from aws_cdk.aws_ec2 import SecurityGroup, InstanceType
from aws_cdk.aws_rds import DatabaseInstance, DatabaseInstanceEngine
import aws_cdk.aws_elasticache as elasticache
from airflow_stack.secret_value import SecretValueFix


class RdsElasticacheStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, deploy_env: str, vpc: aws_ec2.Vpc, config: dict, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        self.config = config
        self.db_sg = SecurityGroup(self, f"AirflowPostgresSG-{deploy_env}", vpc=vpc)
        db_pwd_secret = SecretValueFix(config["db_pwd_secret_arn"], "postgres_pwd")
        self.postgres_db = DatabaseInstance(self, f"AirflowPostgresDb-{deploy_env}", engine=DatabaseInstanceEngine.POSTGRES,
                                       multi_az=self.config["mutli_az_db"],
                                       enable_performance_insights=self.config["db_enable_performance_insights"],
                                       allocated_storage=10,
                                       instance_class=InstanceType(config["db_instance_type"]),
                                       master_username=self.config["dbadmin"],
                                       master_user_password=db_pwd_secret,
                                       vpc=vpc,  # default placement is private subnets
                                       security_groups=[self.db_security_group])
        self.redis_sg = SecurityGroup(self, f"AirflowRedisSG-{deploy_env}", vpc=vpc)
        redis_subnet_group_name = f"AirflowRedisSubnetGrp-{deploy_env}"
        redis_subnet_group = elasticache.CfnSubnetGroup(self, redis_subnet_group_name,
                                                        subnet_ids=[s.subnet_id for s in vpc.private_subnets],
                                                        description="Airflow Redis Cache Subnet Group",
                                                        cache_subnet_group_name=redis_subnet_group_name)
        self.redis = elasticache.CfnCacheCluster(self, f"AirflowRedis-{deploy_env}",
                                            cache_node_type=config["cache_node_type"],
                                            engine="redis", num_cache_nodes=config["num_cache_nodes"],
                                            az_mode=config["cache_az_mode"],
                                            vpc_security_group_ids=[self.redis_security_group.security_group_id],
                                            cache_subnet_group_name=redis_subnet_group_name)
        self.redis.add_depends_on(redis_subnet_group)

    @property
    def redis_host(self):
        return self.redis.attr_redis_endpoint_address

    @property
    def db_host(self):
        return self.postgres_db.db_instance_endpoint_address

    @property
    def db_security_group(self):
        return self.db_sg

    @property
    def redis_security_group(self):
        return self.redis_sg