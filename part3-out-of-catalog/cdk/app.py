#!/usr/bin/env python3
import os
import aws_cdk as cdk
from ooc.vpc import CdkVpcStack
from ooc.opensearch import CdkOpenSearchStack
from cdk_nag import AwsSolutionsChecks,NagSuppressions,NagPackSuppression

app = cdk.App()
vpc_stack = CdkVpcStack(app, "cdk-vpc")
os_stack = CdkOpenSearchStack(app, "cdk-opensearch", vpc=vpc_stack.vpc)
cdk.Aspects.of(app).add(AwsSolutionsChecks())

NagSuppressions.add_stack_suppressions (os_stack, 
    suppressions=[
        NagPackSuppression (
            id="AwsSolutions-IAM4",
            reason="CDK applies BasicExecutionRole to functions."
        ),
        NagPackSuppression (
            id="AwsSolutions-L1",
            reason="CDK using latest run times."
        )
    ]
) 

app.synth()
