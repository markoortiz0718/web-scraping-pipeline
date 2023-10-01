import aws_cdk as cdk

from web_scraping_pipeline.web_scraping_pipeline_stack import WebScrapingPipelineStack

app = cdk.App()
environment = app.node.try_get_context("environment")
WebScrapingPipelineStack(
    app,
    "WebScrapingPipelineStack",
    env=cdk.Environment(region=environment["AWS_REGION"]),
    environment=environment,
)
app.synth()
