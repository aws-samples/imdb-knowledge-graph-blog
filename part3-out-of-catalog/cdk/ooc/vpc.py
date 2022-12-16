from aws_cdk import (
    # Duration,
    Stack,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_logs as logs
)
import os
from constructs import Construct
from cdk_nag import NagSuppressions, NagPackSuppression

class CdkVpcStack(Stack):
    def __init__(self, scope: Stack, construct_id=str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        log_group = logs.LogGroup(self, "IMDb-Blog3LogGroup")

        role = iam.Role(self, "IMDb-Blog3-FlowLog-Role",
            assumed_by=iam.ServicePrincipal("vpc-flow-logs.amazonaws.com")
        )
        NagSuppressions.add_resource_suppressions(role, [
            NagPackSuppression(
                id='AwsSolutions-IAM5',
                applies_to= ['Resource::*'],
                reason='Flow log role'
            ),
        ])
        
        self.vpc = ec2.Vpc(
            self,
            "BaseVPC",
            max_azs=1,
            cidr="10.0.0.0/16",
            enable_dns_support=True,
            enable_dns_hostnames=True,
            gateway_endpoints={
                "s3": ec2.GatewayVpcEndpointOptions(
                    service=ec2.GatewayVpcEndpointAwsService.S3
                )
            },
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    subnet_type=ec2.SubnetType.PUBLIC, name="Public", cidr_mask=24
                ),
                ec2.SubnetConfiguration(
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                    name="Private",
                    cidr_mask=24,
                ),
            ],
            nat_gateways=1,
        )
        self.vpc.add_flow_log("Blog3FlowLogS3",
                destination=ec2.FlowLogDestination.to_cloud_watch_logs(log_group, role)
            )