from aws_cdk import core
from aws_cdk.aws_ec2 import Vpc


class VpcStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, deploy_env: str, config: dict, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        self.config = config
        self.deploy_env = deploy_env
        self.vpc = Vpc(self,  f"AirflowVPC-{deploy_env}", cidr="10.0.0.0/16", max_azs=config["max_vpc_azs"])

    def get_vpc(self):
        return self.vpc