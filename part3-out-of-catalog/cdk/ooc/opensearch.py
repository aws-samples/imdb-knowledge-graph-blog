from aws_cdk import (
    Stack,
    RemovalPolicy,
    Duration,
    CfnOutput,
    CfnParameter,
    Fn,
    custom_resources as cr,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_logs as logs,
    aws_opensearchservice as es,
    aws_apigateway as apigateway,
    aws_events as events,
    aws_lambda as _lambda,
    aws_lambda_python_alpha as _pylambda,  # python -m pip install aws-cdk.aws-lambda-python-alpha
)
from cdk_nag import NagSuppressions, NagPackSuppression
import os
from constructs import Construct


class CdkOpenSearchStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, vpc, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Parameters
        embeddings_file = CfnParameter(
            self, "embeddingsFile", type="String", description="embeddings"
        ).value_as_string
        movie_node_file = CfnParameter(
            self, "movieNodeFile", type="String", description="movie nodes"
        ).value_as_string

        # Get bucket names from params for permissions
        bucket_name_emb_file = Fn.select(2, Fn.split("/", embeddings_file))
        bucket_name_movie_node = Fn.select(2, Fn.split("/", movie_node_file))

        ######################### Amazon OpenSearch Service ####################################
        es_security_group = ec2.SecurityGroup(
            self, "ESSecurityGroup", vpc=vpc, allow_all_outbound=True
        )

        es_domain = es.Domain(
            self,
            "Domain",
            version=es.EngineVersion.OPENSEARCH_1_3,
            vpc=vpc,
            capacity={
                "data_node_instance_type": "r5.large.search",
                "data_nodes": 1,
                "master_nodes": 0,
                "warm_nodes": 0,
            },
            domain_name="ooc-imdb-search",
            vpc_subnets=[
                ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED)
            ],
            security_groups=[es_security_group],
            removal_policy=RemovalPolicy.DESTROY,
            node_to_node_encryption=True,
            encryption_at_rest=es.EncryptionAtRestOptions(
                enabled=True
                ),
            logging=es.LoggingOptions(
                slow_search_log_enabled=True,
                app_log_enabled=True,
                slow_index_log_enabled=True
            ),
            use_unsigned_basic_auth=False
        )
        
        ######################### AWS Lambda ####################################
        read_fn = _pylambda.PythonFunction(
            self,
            "ReadFromOpenSearchLambda",
            entry="ooc/lambdas/ReadFromOpenSearchLambda",
            index="lambda_handler.py",
            handler="lambda_handler",
            runtime=_lambda.Runtime.PYTHON_3_8,
            vpc=vpc,
            environment={
                "opensearch_url": es_domain.domain_endpoint,
            },
            timeout=Duration.minutes(15),
        )

        create_index_fn = _pylambda.PythonFunction(
            self,
            "LoadDataIntoOpenSearchLambda",
            entry="ooc/lambdas/LoadDataIntoOpenSearchLambda",
            index="lambda_handler.py",
            handler="lambda_handler",
            runtime=_lambda.Runtime.PYTHON_3_8,
            vpc=vpc,
            memory_size=10240,
            environment={
                "embeddings_file": embeddings_file,
                "movie_node_file": movie_node_file,
                "opensearch_url": es_domain.domain_endpoint,
            },
            timeout=Duration.minutes(15),
        )

        ########################## IAM Adding policy to roles #####################
        create_index_fn.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3:Get*",
                ],
                resources=[
                    f"arn:aws:s3:::{bucket_name_emb_file}",
                    f"arn:aws:s3:::{bucket_name_emb_file}/*",
                    f"arn:aws:s3:::{bucket_name_movie_node}",
                    f"arn:aws:s3:::{bucket_name_movie_node}/*",
                ],
            )
        )
        
        ######################### Granting Permissions to Lambda ####################################
        es_domain.grant_read_write(read_fn.grant_principal)
        es_domain.connections.allow_from(read_fn, ec2.Port.tcp(443))
        es_domain.grant_read_write(create_index_fn.grant_principal)
        es_domain.connections.allow_from(create_index_fn, ec2.Port.tcp(443))

        ######################### Amazon API Gateway ####################################
        log_group = logs.LogGroup(self, "ApiGatewayAccessLogs")
        
        api = apigateway.RestApi(
            self,
            "search-api",
            rest_api_name="ooc_service",
            description="This service serves ooc.",
            cloud_watch_role=True,
            deploy_options=apigateway.StageOptions(
                access_log_destination=apigateway.LogGroupLogDestination(log_group),
                access_log_format=apigateway.AccessLogFormat.clf(),
                stage_name='dev',
                logging_level=apigateway.MethodLoggingLevel.ERROR
                )
        )
        
        api.add_request_validator("reqValidator",request_validator_name="RequestValidator", validate_request_body=True, validate_request_parameters=True)
        search = api.root.add_resource("opensearch-lambda")

        ooc_integration = apigateway.LambdaIntegration(
            read_fn, request_templates={"application/json": '{ "statusCode": "200" }'}
        )

        search.add_method(
            "GET",
            ooc_integration,
            request_parameters={
                "method.request.querystring.q": True,
                "method.request.querystring.numMovies": True,
                "method.request.querystring.numRecs": True,
            },
            authorization_type=apigateway.AuthorizationType.IAM
        )
        
        ######################## cdk nag supressions ####################################
        NagSuppressions.add_resource_suppressions_by_path(self,
                        "/cdk-opensearch/search-api/Default/opensearch-lambda/GET/Resource",
                        [{"id":'AwsSolutions-COG4',"reason":"Using IAM Auth instead of cognito"}])
        
        NagSuppressions.add_resource_suppressions_by_path(self,
                        "/cdk-opensearch/search-api/DeploymentStage.dev/Resource",
                        [{"id":'AwsSolutions-APIG3',"reason":"suppressing WAF warning"}])
        
        NagSuppressions.add_resource_suppressions(es_domain, [
            {
            "id": "AwsSolutions-IAM5",
            "reason": "cdk generates roles that have more expansive permissions. For production, please use least permission permissions for roles",
            "applies_to": ["Resource::*"]
            },
            NagPackSuppression(
                id='AwsSolutions-OS3',
                reason='Granting more than IP addresses allow listing to be used by AWS Lambda. For production, use allow listing IP addresses only'
            ),
            NagPackSuppression(
                id='AwsSolutions-OS5',
                reason='For the blog, access policies have not been used but the cluster is in VPC. For production, please restrict access further using access policies'
            ),
            NagPackSuppression(
                id='AwsSolutions-OS4',
                reason='master nodes (need more than 1) should be added for production'
            ),
            NagPackSuppression(
                id='AwsSolutions-OS7',
                reason='Single zone awareness for the blog is 1. For production, please use multi zone awareness'
            )
        ],True)
        
        NagSuppressions.add_resource_suppressions_by_path(self,
                                                          "/cdk-opensearch/ReadFromOpenSearchLambda/ServiceRole/DefaultPolicy/Resource",
                                                         [{"id":'AwsSolutions-IAM5',"reason":"cdk generates roles have more expansive permissions. For production, please use least permission permissions for roles"}])
        NagSuppressions.add_resource_suppressions_by_path(self,
                                                          "/cdk-opensearch/LoadDataIntoOpenSearchLambda/ServiceRole/DefaultPolicy/Resource",
                                                         [{"id":'AwsSolutions-IAM5',"reason":"cdk generates roles have more expansive permissions. For production, please use least permission permissions for roles and adding supression for s3::Get* to provide READ access to the customer bucket only"}])
