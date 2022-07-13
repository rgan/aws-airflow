from aws_cdk import Stack
from aws_cdk.aws_ec2 import Vpc
from constructs import Construct


class VpcStack(Stack):

    def __init__(self, scope: Construct, id: str, deploy_env: str, config: dict, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        self.config = config
        self.deploy_env = deploy_env
        self.vpc = Vpc(self,  f"AirflowVPC-{deploy_env}", cidr="10.0.0.0/16", max_azs=config["max_vpc_azs"])

    def get_vpc(self):
        return self.vpc