from aws_cdk import core, aws_ec2, aws_efs
from aws_cdk.aws_ec2 import SecurityGroup, InstanceType, InstanceClass, InstanceSize, AmazonLinuxGeneration, \
    AmazonLinuxEdition, AmazonLinuxStorage, SubnetType, SubnetSelection, MachineImage, Port, Protocol
from aws_cdk.aws_efs import PerformanceMode, ThroughputMode
from aws_cdk.aws_rds import DatabaseInstance, DatabaseInstanceEngine
import aws_cdk.aws_elasticache as elasticache
from aws_cdk.core import SecretValue

REDIS_PORT = 6379
MOUNT_POINT = "/mnt/efs"

class RdsElasticacheEfsStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, deploy_env: str, vpc: aws_ec2.Vpc, config: dict, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        self.config = config
        self.db_sg = SecurityGroup(self, f"AirflowPostgresSG-{deploy_env}", vpc=vpc)
        db_pwd_secret = SecretValue.ssm_secure(self.config["dbadmin_pwd_secret"], "1")
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
        redis_port_info = Port(protocol=Protocol.TCP, string_representation="allow to redis",
                               from_port=REDIS_PORT, to_port=REDIS_PORT)
        file_system_name = f'AirflowEFS-{deploy_env}'
        self.efs_file_system = aws_efs.FileSystem(self, file_system_name, file_system_name=file_system_name,
                                                  vpc=vpc, encrypted=False, performance_mode=PerformanceMode.GENERAL_PURPOSE,
                                                  throughput_mode=ThroughputMode.BURSTING)
        self.bastion = self.setup_bastion_access(self.postgres_db, deploy_env, self.redis_sg, vpc, redis_port_info)
        self.setup_efs_volume()

    def setup_efs_volume(self):
        self.efs_file_system.connections.allow_default_port_from(self.bastion)
        self.bastion.add_user_data("yum check-update -y",
                              "yum upgrade -y",
                              "yum install -y amazon-efs-utils",
                              "yum install -y nfs-utils",
                              "file_system_id_1=" + self.efs_file_system.file_system_id,
                              "efs_mount_point_1="+self.mount_point,
                              "mkdir -p \"${efs_mount_point_1}\"",
                              "test -f \"/sbin/mount.efs\" && echo \"${file_system_id_1}:/ ${efs_mount_point_1} efs defaults,_netdev\" >> /etc/fstab || " +
                              "echo \"${file_system_id_1}.efs." + self.region + ".amazonaws.com:/ ${efs_mount_point_1} nfs4 nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport,_netdev 0 0\" >> /etc/fstab",
                              "mount -a -t efs,nfs4 defaults")

    def setup_bastion_access(self, postgres_db, deploy_env, redis_sg, vpc, redis_port_info):
        self.bastion = aws_ec2.Instance(self, f"AirflowBastion-{deploy_env}", vpc=vpc,
                                   instance_type=InstanceType.of(InstanceClass.BURSTABLE2, InstanceSize.MICRO),
                                   machine_image=MachineImage.latest_amazon_linux(generation=AmazonLinuxGeneration.AMAZON_LINUX,
                                                                                edition=AmazonLinuxEdition.STANDARD,
                                                                                storage=AmazonLinuxStorage.GENERAL_PURPOSE),
                                   vpc_subnets=SubnetSelection(subnet_type=SubnetType.PUBLIC),
                                   key_name="airflow")
        # self.bastion.user_data.add_commands("sudo yum check-update -y", "sudo yum upgrade -y",
        #                                         "sudo yum install postgresql-devel python-devel gcc",
        #                                         "virtualenv env && source env/bin/activate && pip install pgcli==1.11.0")
        ssh_port_info = Port(protocol=Protocol.TCP, string_representation="allow ssh",
                             from_port=22, to_port=22)
        # As an alternative to providing a keyname we can use [EC2 Instance Connect]
        # https://aws.amazon.com/blogs/infrastructure-and-automation/securing-your-bastion-hosts-with-amazon-ec2-instance-connect/
        # with the command `aws ec2-instance-connect send-ssh-public-key` to provide your SSH public key.
        self.bastion.connections.allow_from_any_ipv4(ssh_port_info)
        self.bastion.connections.security_groups[0].connections.allow_to_default_port(postgres_db, 'allow PG')
        self.bastion.connections.security_groups[0].connections.allow_to(redis_sg, redis_port_info, 'allow Redis')
        return self.bastion

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

    @property
    def efs_file_system_id(self):
        return self.efs_file_system.file_system_id

    @property
    def mount_point(self):
        return MOUNT_POINT