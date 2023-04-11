from aws_cdk import (
    # Duration,
    Stack,
    Duration,
    aws_lambda as lmbd,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    aws_lambda_python_alpha as lmbd_python,
    aws_rds as rds,
    aws_ec2 as ec2
    # aws_sqs as sqs,
)
from constructs import Construct


class MapDataResources(Construct):
    def __init__(self, scope: Construct, construct_id: str, resources, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        MapDataFn = lmbd_python.PythonFunction(
            self, "MapDataFn",
            entry="scout_pipeline_aws/lambdas/mapData",
            runtime=lmbd.Runtime.PYTHON_3_9,
            memory_size=1920,
            timeout=Duration.minutes(10),
            environment={
                "bucket_name": resources.cdl_scout_aws_bucket.bucket_name
            },
        )
        resources.cdl_scout_aws_bucket.grant_read_write(
            MapDataFn.role)
        resources.cdl_scout_bucket.grant_read(MapDataFn.role)


class QADataResources(Construct):
    def __init__(self, scope: Construct, construct_id: str, resources, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        QADashboardDataLoadFn = lmbd_python.PythonFunction(
            self, "QADataFn",
            entry="scout_pipeline_aws/lambdas/QAData",
            runtime=lmbd.Runtime.PYTHON_3_9,
            memory_size=1920,
            timeout=Duration.minutes(10),
            vpc=ec2.Vpc.from_vpc_attributes(
                self, 'bidDefaultVpc', availability_zones=["us-east-2"], vpc_id="vpc-082b49befe69b73c4"),
            vpc_subnets=ec2.SubnetSelection(subnets=[
                ec2.Subnet.from_subnet_id(
                    self, 'subnet1', "subnet-0921b54f3f82ab217"),
                ec2.Subnet.from_subnet_id(
                    self, 'subnet2', "subnet-05ea4689b13e7f258")
            ]),
            environment={
                "user_name" : resources.user_name,
                "database_host": resources.database_host,
                "database_name": resources.database_name,
                "database_port": '3306',
                "database_password": resources.database_password
            },
            
        )
        resources.cdl_scout_bucket.grant_read(QADashboardDataLoadFn.role)

class MatchLoadDataResources(Construct):
    def __init__(self, scope: Construct, construct_id: str, resources, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        MatchLoadDataFn = lmbd_python.PythonFunction(
            self, "MatchLoadDataFn",
            entry="scout_pipeline_aws/lambdas/matchLoadData",
            runtime=lmbd.Runtime.PYTHON_3_9,
            memory_size=10000,
            timeout=Duration.minutes(10),
            vpc=ec2.Vpc.from_vpc_attributes(
                self, 'bidDefaultVpc', availability_zones=["us-east-2"], vpc_id="vpc-082b49befe69b73c4"),
            vpc_subnets=ec2.SubnetSelection(subnets=[
                ec2.Subnet.from_subnet_id(
                    self, 'subnet1', "subnet-0921b54f3f82ab217"),
                ec2.Subnet.from_subnet_id(
                    self, 'subnet2', "subnet-05ea4689b13e7f258")
            ]),
            environment={
                "user_name" : resources.user_name,
                "database_host": resources.database_host,
                "database_name": resources.database_name,
                "database_port": '3306',
                "database_password": resources.database_password
            },
            
        )
        resources.cdl_scout_bucket.grant_read_write(MatchLoadDataFn.role)

class NielsenDataResources(Construct):
    def __init__(self, scope: Construct, construct_id: str, resources, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        NielsenDataFn = lmbd_python.PythonFunction(
            self, "NielsenDataFn",
            entry="scout_pipeline_aws/lambdas/nielsenData",
            runtime=lmbd.Runtime.PYTHON_3_9,
            memory_size=5000,
            timeout=Duration.minutes(10),
            vpc=ec2.Vpc.from_vpc_attributes(
                self, 'bidDefaultVpc', availability_zones=["us-east-2"], vpc_id="vpc-082b49befe69b73c4"),
            vpc_subnets=ec2.SubnetSelection(subnets=[
                ec2.Subnet.from_subnet_id(
                    self, 'subnet1', "subnet-0921b54f3f82ab217"),
                ec2.Subnet.from_subnet_id(
                    self, 'subnet2', "subnet-05ea4689b13e7f258")
            ]),
            environment={
                "user_name" : resources.user_name,
                "database_host": resources.database_host,
                "database_name": resources.database_name,
                "database_port": '3306',
                "database_password": resources.database_password
            },
            
        )
        resources.cdl_scout_bucket.grant_read(NielsenDataFn.role)

class SharedResources(Construct):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.cdl_scout_aws_bucket = s3.Bucket(self, "cdl-scout-aws")
        self.cdl_scout_bucket = s3.Bucket.from_bucket_name(
            self, "cdl-scout", "cdl-scout")
        self.user_name = 'gurbir'
        self.database_host = 'competitivelandscape.cmnoiok79kts.us-east-2.rds.amazonaws.com'
        self.database_password = 'C0mp8sspassword314159'
        self.database_name = 'competitivelandscape'



class ScoutPipelineAwsStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here
        sharedObject = SharedResources(self, 'SharedDataResourcesId')
        MapDataObject = MapDataResources(
            self, "MapDataResourcesId", sharedObject)
        QADataObject = QADataResources(
            self, "QADataResourcesId", sharedObject)
        MatchLoadDataObject = MatchLoadDataResources(
            self, "MatchLoadDataResourcesId", sharedObject)
        NielsenDataDataObject = NielsenDataResources(
            self, "NielsenDataResourcesId", sharedObject)
        # example resource
        # queue = sqs.Queue(
        #     self, "ScoutPipelineAwsQueue",
        #     visibility_timeout=Duration.seconds(300),
        # )
