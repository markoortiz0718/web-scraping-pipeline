from aws_cdk import (
    BundlingOptions,
    Duration,
    RemovalPolicy,
    Stack,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_lambda_event_sources as _lambda_event_sources,
    aws_redshift as redshift,
    aws_s3 as s3,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subscriptions,
    aws_sqs as sqs,
)
from constructs import Construct


class PublishWebScrapedMessagesService(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: dict,
    ) -> None:
        super().__init__(scope, construct_id)  # required

        # stateful resources
        self.scheduled_eventbridge_event = events.Rule(
            self,
            "RunEveryMinute",
            event_bus=None,  # scheduled events must be on "default" bus
            schedule=events.Schedule.rate(Duration.minutes(1)),
        )
        self.scraped_messages_topic = sns.Topic(self, "ScapedMessagesTopic")

        # stateless resources
        self.publish_messages_to_sns_lambda = _lambda.Function(
            self,
            "PublishMessagesToSNSLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "lambda_code/publish_messages_to_sns_lambda",
                # exclude=[".venv/*"],  # seems to no longer do anything if use BundlingOptions
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        " && ".join(
                            [
                                "cp handler.py data-engineering-bezant-assignement-dataset.zip /asset-output",  # need to cp instead of mv
                                "pip install -r requirements.txt -t /asset-output",
                            ]
                        ),
                    ],
                ),
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(1),  # should be fairly quick
            memory_size=1024,  # in MB
        )

        # connect AWS resources
        self.scheduled_eventbridge_event.add_target(
            target=events_targets.LambdaFunction(
                handler=self.publish_messages_to_sns_lambda,
                retry_attempts=3,
                ### then put in DLQ
            ),
        )
        self.scraped_messages_topic.grant_publish(self.publish_messages_to_sns_lambda)
        self.publish_messages_to_sns_lambda.add_environment(
            key="TOPIC_ARN", value=self.scraped_messages_topic.topic_arn
        )


class WriteMessagesToRedshiftService(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: dict,
    ) -> None:
        super().__init__(scope, construct_id)  # required

        # stateful resources
        self.scraped_messages_queue_for_redshift = sqs.Queue(
            self,
            "ScrapedMessagesQueue",
            removal_policy=RemovalPolicy.DESTROY,
            retention_period=Duration.days(4),
            visibility_timeout=Duration.seconds(
                30
            ),  # retry failed message within same minute
        )
        self.s3_bucket_for_redshift_staging = s3.Bucket(
            self,
            "S3BucketForRedshiftStaging",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            versioned=False,  # if versioning disabled, then expired files are deleted
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="expire_files_with_certain_prefix_after_1_day",
                    expiration=Duration.days(1),
                    prefix=f"{environment['PROCESSED_SQS_MESSAGES_FOLDER']}/",
                ),
            ],
        )
        self.redshift_full_commands_full_access_role = iam.Role(
            self,
            "RedshiftClusterRole",
            assumed_by=iam.ServicePrincipal("redshift.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonRedshiftAllCommandsFullAccess"
                ),  ### later principle of least privileges
            ],
        )
        self.redshift_cluster = redshift.CfnCluster(
            self,
            "RedshiftCluster",
            cluster_type="single-node",  # for demo purposes
            number_of_nodes=1,  # for demo purposes
            node_type="dc2.large",  # for demo purposes
            db_name=environment["REDSHIFT_DATABASE_NAME"],
            master_username=environment["REDSHIFT_USER"],
            master_user_password=environment["REDSHIFT_PASSWORD"],
            iam_roles=[self.redshift_full_commands_full_access_role.role_arn],
            # cluster_subnet_group_name=demo_cluster_subnet_group.ref,
            # vpc_security_group_ids=[security_group.security_group_id],
        )

        # stateless resources
        self.lambda_redshift_full_access_role = iam.Role(
            self,
            "LambdaRedshiftFullAccessRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonRedshiftFullAccess"
                ),  ### later principle of least privileges
            ],
        )
        self.write_messages_to_redshift_lambda = _lambda.Function(
            self,
            "WriteMessagesToRedshiftLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "lambda_code/write_messages_to_redshift_lambda",
                # exclude=[".venv/*"],  # seems to no longer do anything if use BundlingOptions
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        " && ".join(
                            [
                                "cp handler.py /asset-output",  # need to cp instead of mv
                                "pip install -r requirements.txt -t /asset-output",
                            ]
                        ),
                    ],
                ),
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(20),  # can take awhile
            memory_size=1024,  # in MB
            environment={
                "AWSREGION": environment[
                    "AWS_REGION"
                ],  # apparently "AWS_REGION" is not allowed as a Lambda env variable
                "UNPROCESSED_SQS_MESSAGES_FOLDER": environment[
                    "UNPROCESSED_SQS_MESSAGES_FOLDER"
                ],
                "PROCESSED_SQS_MESSAGES_FOLDER": environment[
                    "PROCESSED_SQS_MESSAGES_FOLDER"
                ],
                "REDSHIFT_USER": environment["REDSHIFT_USER"],
                "REDSHIFT_DATABASE_NAME": environment["REDSHIFT_DATABASE_NAME"],
                "REDSHIFT_SCHEMA_NAME": environment["REDSHIFT_SCHEMA_NAME"],
                "REDSHIFT_TABLE_NAME": environment["REDSHIFT_TABLE_NAME"],
            },
            role=self.lambda_redshift_full_access_role,
        )

        # connect AWS resources
        self.write_messages_to_redshift_lambda.add_event_source(
            _lambda_event_sources.SqsEventSource(
                self.scraped_messages_queue_for_redshift, batch_size=1
            )
        )
        self.s3_bucket_for_redshift_staging.grant_read_write(
            self.write_messages_to_redshift_lambda
        )
        lambda_environment_variables = {
            "REDSHIFT_ENDPOINT_ADDRESS": self.redshift_cluster.attr_endpoint_address,
            "REDSHIFT_ROLE_ARN": self.redshift_full_commands_full_access_role.role_arn,
            "S3_BUCKET_FOR_REDSHIFT_STAGING": self.s3_bucket_for_redshift_staging.bucket_name,
        }
        for key, value in lambda_environment_variables.items():
            self.write_messages_to_redshift_lambda.add_environment(key=key, value=value)


class WebScrapingPipelineStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: dict,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.publish_web_scraped_messages_service = PublishWebScrapedMessagesService(
            self, "PublishWebScrapedMessagesService", environment=environment
        )
        self.write_messages_to_redshift_service = WriteMessagesToRedshiftService(
            self, "WriteMessagesToRedshiftService", environment=environment
        )

        # connect AWS resources
        self.publish_web_scraped_messages_service.scraped_messages_topic.add_subscription(
            sns_subscriptions.SqsSubscription(
                self.write_messages_to_redshift_service.scraped_messages_queue_for_redshift
            )
        )
        self.publish_web_scraped_messages_service.scheduled_eventbridge_event.node.add_dependency(
            self.write_messages_to_redshift_service.write_messages_to_redshift_lambda
        )  # make sure that Lambda is created before Eventbridge rule to prevent race condition
