from aws_cdk.core import SecretValue


class SecretValueFix(SecretValue):

    def __init__(self, secret_arn, fieldname):
        super().__init__(secret_arn)
        self.secret_arn = secret_arn
        self.fieldname = fieldname

    def to_string(self) -> str:
        return  "{{resolve:secretsmanager:{0}::{1}}}".format(self.secret_arn, self.fieldname)