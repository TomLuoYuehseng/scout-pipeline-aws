import aws_cdk as core
import aws_cdk.assertions as assertions

from scout_pipeline_aws.scout_pipeline_aws_stack import ScoutPipelineAwsStack

# example tests. To run these tests, uncomment this file along with the example
# resource in scout_pipeline_aws/scout_pipeline_aws_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = ScoutPipelineAwsStack(app, "scout-pipeline-aws")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
