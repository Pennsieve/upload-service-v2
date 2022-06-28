# Platform Events SQS Cloudwatch DLQ Alarm
resource "aws_cloudwatch_metric_alarm" "upload_sqs_dlq_cloudwatch_metric_alarm" {
  alarm_name                = "${var.environment_name}-upload-sqs-deadletter-queue-alarm-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "ApproximateNumberOfMessagesVisible"
  namespace                 = "AWS/SQS"
  period                    = "60"
  statistic                 = "Average"
  threshold                 = "50"
  alarm_description         = "This metric monitors Upload-V2 SQS DLQ for messages"
  insufficient_data_actions = []
  alarm_actions             = [data.terraform_remote_state.account.outputs.data_management_victor_ops_sns_topic_id]
  ok_actions                = [data.terraform_remote_state.account.outputs.data_management_victor_ops_sns_topic_id]
  treat_missing_data        = "ignore"

  dimensions = {
    QueueName = aws_sqs_queue.upload_trigger_deadletter_queue.name
  }
}

// CREATE FARGATE TASK CLOUDWATCH LOG GROUP
resource "aws_cloudwatch_log_group" "fargate_cloudwatch_log_group" {
  name              = "/aws/fargate/${var.environment_name}-${var.service_name}-${var.tier}-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  retention_in_days = 7

  tags = local.common_tags
}