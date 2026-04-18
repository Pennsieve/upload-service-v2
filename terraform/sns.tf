resource "aws_sns_topic" "imported_file_sns_topic" {
  name         = "${var.environment_name}-${var.service_name}-imported-file-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  display_name = "${var.environment_name}-${var.service_name}-imported-file-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
}

// CREATE SNS TOPIC SUBSCRIPTION
resource "aws_sns_topic_subscription" "sns_topic_subscription" {
  topic_arn = aws_sns_topic.imported_file_sns_topic.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.imported_file_queue.arn
}

resource "aws_sns_topic_policy" "imported_file_sns_topic_policy" {
  arn    = aws_sns_topic.imported_file_sns_topic.arn
  policy = data.aws_iam_policy_document.imported_file_sns_topic_iam_policy.json
}

data "aws_iam_policy_document" "imported_file_sns_topic_iam_policy" {

  statement {
    sid    = "AllowLambda"
    effect = "Allow"

    resources = [aws_sns_topic.imported_file_sns_topic.arn]

    actions = ["sns:Publish"]

    principals {
      identifiers = ["lambda.amazonaws.com"]
      type        = "Service"
    }
  }
}

# Ops alerts topic for the reconcile + archive-sweeper lambdas. Operators
# subscribe to this topic (email / Slack / etc.) to get notified when the
# CloudWatch alarms defined in cloudwatch.tf trigger.
resource "aws_sns_topic" "reconcile_alerts" {
  name = "${var.environment_name}-${var.service_name}-reconcile-alerts-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
}
